from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataFetcher:
    """Unified data fetcher with tushare/qlib + local stub fallback."""

    def __init__(self, config: dict) -> None:
        self.config = config
        settings = config.get('settings', config)
        data_cfg = settings.get('data', {})
        self.provider = data_cfg.get('provider', 'local_stub')
        self.fallback_provider = data_cfg.get('fallback_provider', 'local_stub')
        self._qlib_inited = False
        self._tushare_inited = False
        self._tushare_pro = None
        self._sqlite_inited = False
        self._sqlite_db_path = ''
        self._sqlite_table = 'daily_trading_data'

        if self.provider == 'tushare':
            self._try_init_tushare(data_cfg)
        elif self.provider == 'qlib':
            self._try_init_qlib(data_cfg)
        elif self.provider == 'sqlite':
            self._try_init_sqlite(data_cfg)

    def _resolve_data_path(self, raw_path: str) -> Path:
        path = Path(raw_path).expanduser()
        if path.is_absolute():
            return path
        project_root = Path(__file__).resolve().parents[2]
        return (project_root / path).resolve()

    def _try_init_sqlite(self, data_cfg: dict) -> None:
        raw_path = str(data_cfg.get('sqlite_db_path', '')).strip()
        if not raw_path:
            logger.warning('SQLite provider selected but sqlite_db_path is empty, fallback to %s', self.fallback_provider)
            self.provider = self.fallback_provider
            return
        db_path = self._resolve_data_path(raw_path)
        if not db_path.exists() or not db_path.is_file():
            logger.warning('SQLite db not found (%s), fallback to %s', db_path, self.fallback_provider)
            self.provider = self.fallback_provider
            return
        table = str(data_cfg.get('sqlite_table', 'daily_trading_data')).strip() or 'daily_trading_data'
        try:
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            ok = cur.fetchone() is not None
            conn.close()
            if not ok:
                logger.warning('SQLite table %s not found in %s, fallback to %s', table, db_path, self.fallback_provider)
                self.provider = self.fallback_provider
                return
            self._sqlite_inited = True
            self._sqlite_db_path = str(db_path)
            self._sqlite_table = table
            logger.info('SQLite initialised – db=%s table=%s', db_path, table)
        except Exception as e:
            logger.warning('SQLite init failed (%s), fallback to %s', e, self.fallback_provider)
            self.provider = self.fallback_provider

    def _fetch_by_fallback(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        if self._tushare_inited:
            return self._fetch_tushare_stock(ts_code, start_date, end_date)
        if self._qlib_inited:
            return self._fetch_qlib(ts_code, start_date, end_date)
        return self._fetch_stub(ts_code, start_date, end_date)

    def _try_init_qlib(self, data_cfg: dict) -> None:
        try:
            import qlib
            from qlib.config import REG_CN
            provider_uri = str(Path(data_cfg.get('qlib_provider_uri', '~/.qlib/qlib_data/cn_data')).expanduser())
            qlib_init = getattr(qlib, 'init', None)
            if qlib_init is None:
                from qlib import init as qlib_init  # type: ignore
            qlib_init(provider_uri=provider_uri, region=REG_CN)
            self._qlib_inited = True
            logger.info('Qlib initialised – provider_uri=%s', provider_uri)
        except Exception as e:
            logger.warning('Qlib init failed (%s), falling back to %s', e, self.fallback_provider)
            self.provider = self.fallback_provider

    def _resolve_tushare_tokens(self, data_cfg: dict) -> list[str]:
        tokens: list[str] = []

        def _push(token: str | None) -> None:
            if not token:
                return
            t = token.strip()
            if t and t not in tokens:
                tokens.append(t)

        cfg_token = data_cfg.get('tushare_token')
        _push(str(cfg_token) if cfg_token else None)

        env_token = os.getenv('TUSHARE_TOKEN')
        _push(env_token)

        project_root = Path(__file__).resolve().parents[2]
        candidates = []
        token_file = data_cfg.get('tushare_token_file')
        if token_file:
            candidates.append((project_root / token_file).resolve())
        candidates.extend([
            project_root / '.tushare_token',
            project_root / 'tushare_token.txt',
            project_root.parent / '.tushare_token',
            project_root.parent / 'tushare_token.txt',
            Path.home() / '.tushare_token',
            Path.home() / 'tushare_token.txt',
            project_root / '.env',
            project_root.parent / '.env',
        ])
        for file_path in candidates:
            for token in self._read_token_file(file_path):
                _push(token)
        return tokens

    def _read_token_file(self, file_path: Path) -> list[str]:
        if not file_path.exists() or not file_path.is_file():
            return []
        tokens: list[str] = []
        try:
            text = file_path.read_text(encoding='utf-8').strip()
            if not text:
                return []
            is_env_like = file_path.name in {'.env', '.env.local'}
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    key = k.strip().upper()
                    if key in {'TUSHARE_TOKEN', 'TS_TOKEN'} and v.strip():
                        tokens.append(v.strip().strip('"').strip("'"))
                    continue
                if not is_env_like:
                    tokens.append(line.strip().strip('"').strip("'"))
        except Exception:
            return []
        return tokens

    def _classify_tushare_error(self, exc: Exception) -> str:
        msg = str(exc or "").strip()
        lowered = msg.lower()
        if '没有接口访问权限' in msg or 'no permission' in lowered or 'permission denied' in lowered:
            return 'permission_denied'
        if 'token不对' in msg or 'token invalid' in lowered or 'invalid token' in lowered:
            return 'invalid_token'
        if 'read timed out' in lowered or 'connection' in lowered or 'network' in lowered or 'dns' in lowered:
            return 'network_error'
        return 'unknown'

    def _format_tushare_error(self, exc: Exception) -> str:
        msg = str(exc or "").strip() or repr(exc)
        reason = self._classify_tushare_error(exc)
        if reason == 'permission_denied':
            return f'Tushare account lacks API permission ({msg})'
        if reason == 'invalid_token':
            return f'Tushare token invalid ({msg})'
        if reason == 'network_error':
            return f'Tushare network error ({msg})'
        return f'Tushare init failed ({msg})'

    def _try_init_tushare(self, data_cfg: dict) -> None:
        try:
            import tushare as ts
        except Exception as e:
            logger.warning('Tushare import failed (%s), falling back to %s', e, self.fallback_provider)
            self.provider = self.fallback_provider
            return

        tokens = self._resolve_tushare_tokens(data_cfg)
        if not tokens:
            logger.warning('Tushare token not found, falling back to %s', self.fallback_provider)
            self.provider = self.fallback_provider
            return

        for i, token in enumerate(tokens, start=1):
            try:
                ts.set_token(token)
                pro = ts.pro_api(token)
                # Validate token early to avoid silent fallback to stub later.
                _ = pro.trade_cal(exchange='SSE', start_date='20240101', end_date='20240110')
                self._tushare_pro = pro
                self._tushare_inited = True
                logger.info('Tushare initialised successfully (token #%d)', i)
                return
            except Exception as e:
                logger.warning('Tushare token #%d unavailable (%s)', i, self._format_tushare_error(e))

        logger.warning('All Tushare tokens invalid, falling back to %s', self.fallback_provider)
        self.provider = self.fallback_provider

    # -- public API ----------------------------------------------------------

    def fetch_stock_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        if self._sqlite_inited:
            return self._fetch_sqlite_stock(ts_code, start_date, end_date)
        if self._tushare_inited:
            return self._fetch_tushare_stock(ts_code, start_date, end_date)
        if self._qlib_inited:
            return self._fetch_qlib(ts_code, start_date, end_date)
        return self._fetch_stub(ts_code, start_date, end_date)

    def fetch_index_daily(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        if self._tushare_inited:
            return self._fetch_tushare_index(index_code, start_date, end_date)
        if self._qlib_inited:
            return self._fetch_qlib_index(index_code, start_date, end_date)
        return self._fetch_stub(index_code, start_date, end_date)

    def fetch_sector_daily(self, sector_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        if self._tushare_inited:
            proxy_code = sector_code if '.' in sector_code else '000300.SH'
            return self._fetch_tushare_index(proxy_code, start_date, end_date)
        if self._qlib_inited:
            return self._fetch_qlib_index(sector_code, start_date, end_date)
        return self._fetch_stub(sector_code, start_date, end_date)

    def fetch_stock_pool(self, ts_codes: list[str], start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
        return {code: self.fetch_stock_daily(code, start_date, end_date) for code in ts_codes}

    # -- qlib backend --------------------------------------------------------

    def _ts_to_qlib_instrument(self, ts_code: str) -> str:
        """Convert tushare-style code (000001.SZ) to qlib instrument (SZ000001)."""
        parts = ts_code.split('.')
        if len(parts) == 2:
            return f'{parts[1]}{parts[0]}'
        return ts_code

    def _fetch_qlib(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        from qlib.data import D

        instrument = self._ts_to_qlib_instrument(ts_code)
        fields = ['$open', '$high', '$low', '$close', '$volume', '$factor', '$change']
        try:
            df = D.features([instrument], fields, start_time=start_date, end_time=end_date)
            df = df.reset_index()
            df.columns = ['instrument', 'datetime'] + [f.replace('$', '') for f in fields]
            df = df.rename(columns={'datetime': 'date', 'change': 'pct_chg'})
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df['ts_code'] = ts_code
            df['pre_close'] = df['close'].shift(1)
            df['amount'] = df['open'] * df['volume']
            df['amplitude'] = (df['high'] - df['low']) / df['pre_close'].replace(0, np.nan)
            df['turnover_rate'] = 0.0
            df['is_st'] = 0
            df['is_suspend'] = 0
            df = df.drop(columns=['instrument', 'factor'], errors='ignore')
            return df.dropna(subset=['pre_close']).reset_index(drop=True)
        except Exception as e:
            logger.warning('Qlib fetch failed for %s (%s), using stub', ts_code, e)
            return self._fetch_stub(ts_code, start_date, end_date)

    def _fetch_qlib_index(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        from qlib.data import D

        instrument = self._ts_to_qlib_instrument(index_code)
        fields = ['$close', '$volume']
        try:
            df = D.features([instrument], fields, start_time=start_date, end_time=end_date)
            df = df.reset_index()
            df.columns = ['instrument', 'datetime'] + [f.replace('$', '') for f in fields]
            df = df.rename(columns={'datetime': 'date'})
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df['ts_code'] = index_code
            df = df.drop(columns=['instrument'], errors='ignore')
            return df.reset_index(drop=True)
        except Exception:
            return self._fetch_stub(index_code, start_date, end_date)

    # -- tushare backend -----------------------------------------------------

    @staticmethod
    def _to_tushare_date(date_str: str) -> str:
        return pd.Timestamp(date_str).strftime('%Y%m%d')

    @staticmethod
    def _normalise_pct_chg(series: pd.Series) -> pd.Series:
        if series.dropna().abs().max() > 1.5:
            return series / 100.0
        return series

    def _fetch_sqlite_stock(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            start = self._to_tushare_date(start_date)
            end = self._to_tushare_date(end_date)
            conn = sqlite3.connect(self._sqlite_db_path)
            query = (
                f"SELECT * FROM {self._sqlite_table} "
                "WHERE ts_code=? AND trade_date>=? AND trade_date<=? "
                "ORDER BY trade_date ASC"
            )
            df = pd.read_sql_query(query, conn, params=(ts_code, start, end))
            conn.close()
            if df.empty:
                return self._fetch_by_fallback(ts_code, start_date, end_date)

            rename_map = {
                'trade_date': 'date',
                'open_price': 'open',
                'high_price': 'high',
                'low_price': 'low',
                'close_price': 'close',
                'pre_close_price': 'pre_close',
                'change': 'change_amount',
                'vol': 'volume',
            }
            df = df.rename(columns=rename_map)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
            else:
                df['date'] = pd.date_range(start_date, periods=len(df), freq='B').strftime('%Y-%m-%d')

            # Ensure standard columns exist.
            for c in ['open', 'high', 'low', 'close', 'volume', 'amount', 'turnover_rate', 'pct_chg', 'pre_close']:
                if c not in df.columns:
                    df[c] = np.nan
            if df['pre_close'].isna().all():
                df['pre_close'] = df['close'].shift(1)
            if df['pct_chg'].isna().all():
                df['pct_chg'] = df['close'].pct_change()

            df['ts_code'] = ts_code
            df['pct_chg'] = self._normalise_pct_chg(pd.to_numeric(df['pct_chg'], errors='coerce')).fillna(0.0)
            if 'amount' not in df.columns or df['amount'].isna().all():
                df['amount'] = pd.to_numeric(df['close'], errors='coerce') * pd.to_numeric(df['volume'], errors='coerce')
            df['amplitude'] = (
                (pd.to_numeric(df['high'], errors='coerce') - pd.to_numeric(df['low'], errors='coerce'))
                / pd.to_numeric(df['pre_close'], errors='coerce').replace(0, np.nan)
            )
            if 'is_st' in df.columns:
                df['is_st'] = pd.to_numeric(df['is_st'], errors='coerce').fillna(0).astype(int)
            else:
                df['is_st'] = 0
            df['is_suspend'] = (pd.to_numeric(df['volume'], errors='coerce').fillna(0) <= 0).astype(int)
            df['turnover_rate'] = pd.to_numeric(df['turnover_rate'], errors='coerce').fillna(0.0)
            return df[[
                'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
                'pct_chg', 'volume', 'amount', 'turnover_rate', 'amplitude', 'is_st', 'is_suspend',
            ]].dropna(subset=['date', 'close']).reset_index(drop=True)
        except Exception as e:
            logger.warning('SQLite stock fetch failed for %s (%s), using fallback', ts_code, e)
            return self._fetch_by_fallback(ts_code, start_date, end_date)

    def _fetch_tushare_stock(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            start = self._to_tushare_date(start_date)
            end = self._to_tushare_date(end_date)
            pro = self._tushare_pro
            daily = pro.daily(ts_code=ts_code, start_date=start, end_date=end)
            if daily is None or daily.empty:
                return self._fetch_stub(ts_code, start_date, end_date)

            basic = pro.daily_basic(
                ts_code=ts_code,
                start_date=start,
                end_date=end,
                fields='ts_code,trade_date,turnover_rate',
            )

            daily = daily.rename(columns={'trade_date': 'date'})
            daily['date'] = pd.to_datetime(daily['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            if basic is not None and not basic.empty:
                basic = basic.rename(columns={'trade_date': 'date'})
                basic['date'] = pd.to_datetime(basic['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
                daily = daily.merge(
                    basic[['date', 'turnover_rate']],
                    on='date',
                    how='left',
                    suffixes=('', '_basic'),
                )

            daily = daily.sort_values('date').reset_index(drop=True)
            daily['ts_code'] = ts_code
            daily['pct_chg'] = self._normalise_pct_chg(daily.get('pct_chg', pd.Series(dtype=float))).fillna(0.0)
            daily['amplitude'] = (daily['high'] - daily['low']) / daily['pre_close'].replace(0, np.nan)
            daily['is_st'] = 0
            daily['is_suspend'] = (daily['vol'].fillna(0) <= 0).astype(int) if 'vol' in daily.columns else 0
            daily = daily.rename(columns={'vol': 'volume'})
            if 'turnover_rate' not in daily.columns:
                daily['turnover_rate'] = 0.0
            if 'amount' not in daily.columns:
                daily['amount'] = daily['close'] * daily['volume']
            return daily[[
                'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
                'pct_chg', 'volume', 'amount', 'turnover_rate', 'amplitude', 'is_st', 'is_suspend',
            ]]
        except Exception as e:
            logger.warning('Tushare stock fetch failed for %s (%s), using stub', ts_code, e)
            return self._fetch_stub(ts_code, start_date, end_date)

    def _fetch_tushare_index(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            start = self._to_tushare_date(start_date)
            end = self._to_tushare_date(end_date)
            pro = self._tushare_pro
            idx = pro.index_daily(ts_code=index_code, start_date=start, end_date=end)
            if idx is None or idx.empty:
                return self._fetch_stub(index_code, start_date, end_date)
            idx = idx.rename(columns={'trade_date': 'date', 'vol': 'volume'})
            idx['date'] = pd.to_datetime(idx['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            idx = idx.sort_values('date').reset_index(drop=True)
            idx['ts_code'] = index_code
            if 'volume' not in idx.columns:
                idx['volume'] = 0.0
            return idx[['date', 'ts_code', 'close', 'volume']]
        except Exception as e:
            logger.warning('Tushare index fetch failed for %s (%s), using stub', index_code, e)
            return self._fetch_stub(index_code, start_date, end_date)

    # -- stub backend --------------------------------------------------------

    def _fetch_stub(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        dates = pd.date_range(start_date, end_date, freq='B')
        rng = np.random.default_rng(abs(hash(ts_code)) % (2 ** 32))
        price = 10 + np.cumsum(rng.normal(0, 0.2, len(dates)))
        price = np.maximum(price, 0.5)
        df = pd.DataFrame({
            'date': dates.strftime('%Y-%m-%d'),
            'ts_code': ts_code,
            'open': price,
            'high': price * (1 + rng.uniform(0, 0.02, len(dates))),
            'low': price * (1 - rng.uniform(0, 0.02, len(dates))),
            'close': price,
            'pre_close': np.r_[price[0], price[:-1]],
            'volume': rng.integers(1_000_000, 10_000_000, len(dates)).astype(float),
            'amount': rng.integers(10_000_000, 100_000_000, len(dates)).astype(float),
            'turnover_rate': rng.uniform(0.5, 5.0, len(dates)),
            'is_st': 0,
            'is_suspend': 0,
        })
        df['pct_chg'] = df['close'].pct_change().fillna(0)
        df['amplitude'] = (df['high'] - df['low']) / df['pre_close'].replace(0, 1)
        return df
