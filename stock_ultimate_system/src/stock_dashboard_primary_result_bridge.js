__PRIMARY_RESULT_BRIDGE_BOOTSTRAP_SCRIPT__
    function parseJsonScriptTag(id) {
      const node = document.getElementById(id);
      if (!node) return null;
      try {
        return JSON.parse(node.textContent || '');
      } catch (error) {
        return null;
      }
    }
    function isDebugMode() {
      return window.location.hostname === '127.0.0.1' || window.location.hostname === 'localhost';
    }
    function isValidPrimaryResultPayload(payload) {
      if (!payload || typeof payload !== 'object') return false;
      if ((payload.schema_version || '') !== 'primary_result_v1') return false;
      const stage = String(payload.result_lifecycle_stage || '').trim();
      return Boolean(stage);
    }
    function safeText(value, fallback) {
      if (value === null || value === undefined || value === '') return fallback;
      return String(value);
    }
    function stageNameFromLifecycle(stage) {
      const mapping = {
        L1: '研究中',
        L2: '候选结果',
        L3: '审核阶段结果',
        L4: '执行阶段结果',
        L5: '已沉淀',
      };
      return mapping[String(stage || '').trim().toUpperCase()] || '制度阶段待补充';
    }
    function statusSuffixFromFact(fact) {
      const stage = String(fact.result_lifecycle_stage || '').trim().toUpperCase();
      if (stage === 'L4') {
        const value = fact.execution_status;
        const mapping = {
          queued: '待执行',
          ready: '待执行准备',
          running: '运行中',
          completed: '已完成',
          failed: '执行失败',
          cancelled: '已取消',
        };
        return value == null ? null : (mapping[String(value).toLowerCase()] || String(value));
      }
      if (stage === 'L3') {
        const value = fact.audit_status;
        const mapping = {
          in_review: '审核中',
          passed: '已通过',
          failed: '未通过',
          waived: '已豁免',
        };
        return value == null ? null : (mapping[String(value).toLowerCase()] || String(value));
      }
      if (fact.candidate_status != null) {
        const mapping = {
          candidate: '候选中',
          shortlisted: '已入池',
          rejected: '已驳回',
          expired: '已失效',
        };
        return mapping[String(fact.candidate_status).toLowerCase()] || String(fact.candidate_status);
      }
      if (fact.research_status != null) {
        const mapping = {
          in_progress: '研究中',
          completed: '研究完成',
          suspended: '研究暂停',
          abandoned: '研究终止',
        };
        return mapping[String(fact.research_status).toLowerCase()] || String(fact.research_status);
      }
      return null;
    }
    function riskTextFromFact(fact) {
      const mapping = {
        low: '低风险',
        medium: '中风险',
        high: '高风险',
        critical: 'critical',
      };
      if (fact.risk_level == null) return '风险信息暂缺';
      return mapping[String(fact.risk_level).toLowerCase()] || String(fact.risk_level);
    }
    function disabledReasonFromFact(fact) {
      if (fact.disabled_reason != null && String(fact.disabled_reason).trim()) {
        return String(fact.disabled_reason).trim();
      }
      if (fact.terminal_outcome === 'rejected') {
        return '当前对象已被驳回，不能继续推进。';
      }
      if (fact.terminal_outcome === 'cancelled') {
        return '当前对象已被取消，不能继续推进。';
      }
      if (fact.terminal_outcome === 'expired') {
        return '当前对象已过有效窗口，不能继续推进。';
      }
      if (fact.terminal_outcome === 'superseded') {
        return '当前对象已被新版结果替代，不能继续推进。';
      }
      if (fact.audit_status === 'failed') {
        return '审核未通过，当前不能继续推进。';
      }
      if (fact.promotion_status === 'rejected') {
        return '晋升未通过，当前不能继续推进。';
      }
      if (fact.risk_level === 'high' || fact.risk_level === 'critical') {
        return '当前风险较高，暂不建议继续推进。';
      }
      return null;
    }
    function invalidReasonFromFact(fact) {
      const mapping = {
        expired: '该结果已过有效窗口，当前视为失效。',
        superseded: '该结果已被新版结果替代。',
        rejected: '该结果已被制度驳回。',
        failed: '该结果对应执行路径已失败。',
        cancelled: '该结果对应流程已取消。',
        archived: '该结果已归档保存，不再作为当前推进对象。',
      };
      if (fact.terminal_outcome == null) return null;
      if (fact.invalid_reason != null && String(fact.invalid_reason).trim()) {
        return String(fact.invalid_reason).trim();
      }
      return mapping[String(fact.terminal_outcome).toLowerCase()] || String(fact.terminal_outcome);
    }
    function historyRecordLabel(kind, rawValue) {
      const mappings = {
        execution: {
          queued: '待执行',
          ready: '待执行准备',
          running: '运行中',
          completed: '已完成',
          failed: '执行失败',
          cancelled: '已取消',
        },
        audit: {
          in_review: '审核中',
          passed: '已通过',
          failed: '未通过',
          waived: '已豁免',
        },
        candidate: {
          candidate: '候选中',
          shortlisted: '已入池',
          rejected: '已驳回',
          expired: '已失效',
        },
        research: {
          in_progress: '研究中',
          completed: '研究完成',
          suspended: '研究暂停',
          abandoned: '研究终止',
        },
      };
      const key = rawValue == null ? '' : String(rawValue).toLowerCase();
      return (mappings[kind] && mappings[kind][key]) || String(rawValue);
    }
    function historyRecordFromFact(fact) {
      if (fact.history_summary != null && String(fact.history_summary).trim()) {
        const replacements = {
          '执行记录 ready': '执行记录 待执行准备',
          '执行记录 completed': '执行记录 已完成',
          '审核记录 passed': '审核记录 已通过',
          '研究记录 completed': '研究记录 已完成',
        };
        const segments = String(fact.history_summary).split('；').map((x) => replacements[x.trim()] || x.trim()).filter(Boolean);
        const prefixes = ['执行记录', '审核记录', '候选记录', '研究记录'];
        for (const prefix of prefixes) {
          const found = segments.find((segment) => segment.startsWith(prefix));
          if (found) return found;
        }
        return segments[0] || null;
      }
      if (fact.execution_status != null) return `执行记录：${historyRecordLabel('execution', fact.execution_status)}`;
      if (fact.audit_status != null) return `审核记录：${historyRecordLabel('audit', fact.audit_status)}`;
      if (fact.candidate_status != null) return `候选记录：${historyRecordLabel('candidate', fact.candidate_status)}`;
      if (fact.research_status != null) return `研究记录：${historyRecordLabel('research', fact.research_status)}`;
      return null;
    }
    function historySourceFromFact(fact) {
      const sourceFile = fact.history_source_file != null && String(fact.history_source_file).trim()
        ? String(fact.history_source_file).trim()
        : null;
      const sourceTimestamp = fact.history_source_timestamp != null && String(fact.history_source_timestamp).trim()
        ? String(fact.history_source_timestamp).trim()
        : null;
      const generationMode = fact.history_generation_mode != null && String(fact.history_generation_mode).trim()
        ? String(fact.history_generation_mode).trim()
        : null;
      if (sourceFile && sourceTimestamp) {
        return { sourceFile, sourceTimestamp, generationMode: generationMode || 'degraded' };
      }
      const timestamps = fact.source_timestamps && typeof fact.source_timestamps === 'object'
        ? fact.source_timestamps
        : null;
      if (!timestamps) return null;
      const priority = [];
      if (fact.execution_status != null) priority.push('t1_execution_checklist_latest.json');
      if (fact.audit_status != null) priority.push('governance_audit_latest.json');
      if (fact.candidate_status != null) priority.push('buylist_latest.json', 'candidates_top_latest.csv');
      if (fact.research_status != null) priority.push('daily_research_status_latest.json');
      priority.push(
        'candidates_top_latest.csv',
        'daily_research_status_latest.json',
        'governance_audit_latest.json',
        't1_execution_checklist_latest.json',
        't12_rollback_drill_latest.json',
        'buylist_latest.json'
      );
      const seen = new Set();
      for (const sourceName of priority) {
        if (seen.has(sourceName)) continue;
        seen.add(sourceName);
        const ts = timestamps[sourceName];
        if (ts != null && String(ts).trim() && String(ts).trim() !== '-') {
          return { sourceFile: sourceName, sourceTimestamp: String(ts).trim(), generationMode: 'degraded' };
        }
      }
      return null;
    }
    function historyHintFromFact(hasRecord, hasSource) {
      if (hasRecord) return '仅供参考，当前主阶段仍以制度主字段为准。';
      if (hasSource) return '当前仅展示可确认的历史痕迹，不代表已通过或可执行。';
      return null;
    }
    function historyViewModelFromFact(fact) {
      let slotA = historyRecordFromFact(fact);
      const source = historySourceFromFact(fact);
      const hasSource = Boolean(source && source.sourceFile && source.sourceTimestamp);
      if (slotA == null && hasSource) {
        slotA = '历史记录暂缺';
      }
      let slotB = null;
      if (hasSource) {
        const modeLabel = source.generationMode === 'direct' ? '直接事实' : '降级生成';
        slotB = `来源 ${source.sourceFile} · 同步 ${source.sourceTimestamp} · ${modeLabel}`;
      }
      const slotC = historyHintFromFact(slotA != null && slotA !== '历史记录暂缺', hasSource);
      return {
        visible: slotA != null || slotB != null,
        slotA,
        slotB,
        slotC,
      };
    }
    function comparePrimaryResultFacts(serverFact, apiFact) {
      const fields = __PRIMARY_RESULT_CORE_COMPARE_FIELDS__;
      const diff = {};
      fields.forEach((field) => {
        if (serverFact[field] !== apiFact[field]) {
          diff[field] = [serverFact[field], apiFact[field]];
        }
      });
      return diff;
    }
    let primaryResultEffectiveFact = null;
    let primaryResultLiveStatusTimer = null;
    const primaryResultUiState = {
      historyExpanded: true,
      disabledExpanded: true,
      invalidExpanded: true,
    };
    function buildPrimaryResultCardViewModelFromFact(fact) {
      const stageLabel = stageNameFromLifecycle(fact.result_lifecycle_stage);
      const statusSuffix = statusSuffixFromFact(fact);
      const stageCombinedLabel = statusSuffix ? `${stageLabel}（${statusSuffix}）` : stageLabel;
      const historyVM = historyViewModelFromFact(fact);
      const disabledText = disabledReasonFromFact(fact);
      const invalidText = invalidReasonFromFact(fact);
      return {
        ts_code: safeText(fact.ts_code, '对象信息暂缺'),
        stock_name: safeText(fact.stock_name, '名称信息暂缺'),
        stage_label: stageLabel,
        stage_combined_label: stageCombinedLabel,
        result_type_label: safeText(fact.result_type, '-'),
        risk_label: riskTextFromFact(fact),
        sync_note: (() => {
          const raw = safeText(fact.data_sync_note, '同步信息暂缺。');
          if (raw.startsWith('降级显示：')) return '降级说明';
          if (String(raw).toLowerCase().includes('batch_prediction_timeout')) return '当前结果仍带补证痕迹';
          return raw;
        })(),
        source_timestamp: (() => {
          const sourceValues = fact.source_timestamps && typeof fact.source_timestamps === 'object'
            ? Object.values(fact.source_timestamps).filter((x) => x && x !== '-')
            : [];
          return sourceValues.length ? sourceValues.sort().slice(-1)[0] : '-';
        })(),
        history_visible: Boolean(historyVM.visible),
        history_slot_a: historyVM.slotA,
        history_slot_b: historyVM.slotB,
        history_slot_c: historyVM.slotC,
        disabled_visible: Boolean(disabledText),
        disabled_text: disabledText,
        invalid_visible: Boolean(invalidText),
        invalid_text: invalidText,
      };
    }
    function applyPrimaryResultCollapsibleState() {
      const config = [
        ['history', primaryResultUiState.historyExpanded],
        ['disabled', primaryResultUiState.disabledExpanded],
        ['invalid', primaryResultUiState.invalidExpanded],
      ];
      config.forEach(([key, expanded]) => {
        const body = document.getElementById(`primary-result-${key}-body`);
        const toggle = document.getElementById(`primary-result-${key}-toggle`);
        if (body) {
          body.classList.toggle('is-collapsed', !expanded);
          body.setAttribute('aria-hidden', expanded ? 'false' : 'true');
        }
        if (toggle) {
          toggle.textContent = expanded ? '折叠' : '展开';
          toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        }
      });
    }
    function announcePrimaryResultStatus(message) {
      const liveEl = document.getElementById('primary-result-live-status');
      if (!liveEl) return;
      if (primaryResultLiveStatusTimer) {
        window.clearTimeout(primaryResultLiveStatusTimer);
        primaryResultLiveStatusTimer = null;
      }
      liveEl.textContent = '';
      window.setTimeout(() => {
        liveEl.textContent = message;
        primaryResultLiveStatusTimer = window.setTimeout(() => {
          liveEl.textContent = '';
          primaryResultLiveStatusTimer = null;
        }, 1600);
      }, 0);
    }
    async function copyPrimaryResultText(text) {
      if (text == null) return false;
      try {
        await navigator.clipboard.writeText(text);
        return true;
      } catch (_error) {
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.setAttribute('readonly', 'readonly');
        textarea.style.position = 'absolute';
        textarea.style.left = '-9999px';
        document.body.appendChild(textarea);
        textarea.select();
        let copied = false;
        try {
          copied = document.execCommand('copy');
        } catch (_fallbackError) {
          copied = false;
        }
        document.body.removeChild(textarea);
        return copied;
      }
    }
    function primaryResultSummaryTextFromFact(fact) {
      const lines = [
        `对象: ${safeText(fact.ts_code, 'null')} ${safeText(fact.stock_name, 'null')}`,
        `主阶段: ${fact.result_lifecycle_stage == null ? 'null' : String(fact.result_lifecycle_stage)}`,
        `分类: ${fact.result_type == null ? 'null' : String(fact.result_type)}`,
        `风险: ${fact.risk_level == null ? 'null' : String(fact.risk_level)}`,
        `审核状态: ${fact.audit_status == null ? 'null' : String(fact.audit_status)}`,
        `执行状态: ${fact.execution_status == null ? 'null' : String(fact.execution_status)}`,
        `终局处置: ${fact.terminal_outcome == null ? 'null' : String(fact.terminal_outcome)}`,
        `同步说明: ${fact.data_sync_note == null ? 'null' : String(fact.data_sync_note)}`,
      ];
      return lines.join('\n');
    }
    function bindPrimaryResultInteractions() {
      const root = document.getElementById('primary-result-card');
      if (!root) return;
      const bindings = [
        ['history', 'historyExpanded'],
        ['disabled', 'disabledExpanded'],
        ['invalid', 'invalidExpanded'],
      ];
      bindings.forEach(([key, stateKey]) => {
        const toggle = document.getElementById(`primary-result-${key}-toggle`);
        if (!toggle || toggle.dataset.bound === 'true') return;
        toggle.dataset.bound = 'true';
        toggle.addEventListener('click', () => {
          primaryResultUiState[stateKey] = !primaryResultUiState[stateKey];
          applyPrimaryResultCollapsibleState();
        });
      });
      const copySummaryBtn = document.getElementById('primary-result-copy-summary');
      if (copySummaryBtn && copySummaryBtn.dataset.bound !== 'true') {
        copySummaryBtn.dataset.bound = 'true';
        copySummaryBtn.addEventListener('click', async () => {
          if (!primaryResultEffectiveFact) return;
          const ok = await copyPrimaryResultText(primaryResultSummaryTextFromFact(primaryResultEffectiveFact));
          announcePrimaryResultStatus(ok ? '已复制制度事实摘要。' : '复制失败，请重试。');
        });
      }
      const copyJsonBtn = document.getElementById('primary-result-copy-json');
      if (copyJsonBtn && copyJsonBtn.dataset.bound !== 'true') {
        copyJsonBtn.dataset.bound = 'true';
        copyJsonBtn.addEventListener('click', async () => {
          if (!primaryResultEffectiveFact) return;
          const ok = await copyPrimaryResultText(JSON.stringify(primaryResultEffectiveFact, null, 2));
          announcePrimaryResultStatus(ok ? '已复制当前事实 JSON。' : '复制失败，请重试。');
        });
      }
      applyPrimaryResultCollapsibleState();
    }
    function renderPrimaryResultCardSlots(viewModel) {
      const codeEl = document.getElementById('primary-result-code');
      const nameEl = document.getElementById('primary-result-name');
      const stageEl = document.getElementById('primary-result-stage');
      const typeEl = document.getElementById('primary-result-type');
      const riskEl = document.getElementById('primary-result-risk');
      const syncNoteEl = document.getElementById('primary-result-sync-note');
      const sourceTsEl = document.getElementById('primary-result-source-ts');
      const historyEl = document.getElementById('primary-result-history');
      const historySlotAEl = document.getElementById('primary-result-history-slot-a');
      const historySlotBEl = document.getElementById('primary-result-history-slot-b');
      const historySlotCEl = document.getElementById('primary-result-history-slot-c');
      const disabledEl = document.getElementById('primary-result-disabled');
      const disabledTextEl = document.getElementById('primary-result-disabled-text');
      const invalidEl = document.getElementById('primary-result-invalid');
      const invalidTextEl = document.getElementById('primary-result-invalid-text');
      if (codeEl) codeEl.textContent = viewModel.ts_code;
      if (nameEl) nameEl.textContent = viewModel.stock_name;
      if (stageEl) stageEl.textContent = viewModel.stage_combined_label;
      if (typeEl) typeEl.textContent = `分类 ${viewModel.result_type_label}`;
      if (riskEl) riskEl.textContent = viewModel.risk_label;
      if (syncNoteEl) syncNoteEl.textContent = viewModel.sync_note;
      if (sourceTsEl) sourceTsEl.textContent = `最近来源时间 ${viewModel.source_timestamp}`;
      if (historyEl) {
        historyEl.style.display = viewModel.history_visible ? '' : 'none';
      }
      if (historySlotAEl) {
        historySlotAEl.textContent = viewModel.history_slot_a || '';
        historySlotAEl.style.display = viewModel.history_slot_a ? '' : 'none';
      }
      if (historySlotBEl) {
        historySlotBEl.textContent = viewModel.history_slot_b || '';
        historySlotBEl.style.display = viewModel.history_slot_b ? '' : 'none';
      }
      if (historySlotCEl) {
        historySlotCEl.textContent = viewModel.history_slot_c || '';
        historySlotCEl.style.display = viewModel.history_slot_c ? '' : 'none';
      }
      if (disabledEl) {
        disabledEl.style.display = viewModel.disabled_visible ? '' : 'none';
        if (disabledTextEl) {
          disabledTextEl.textContent = viewModel.disabled_visible ? (viewModel.disabled_text || '') : '';
        }
      }
      if (invalidEl) {
        invalidEl.style.display = viewModel.invalid_visible ? '' : 'none';
        if (invalidTextEl) {
          invalidTextEl.textContent = viewModel.invalid_visible ? (viewModel.invalid_text || '') : '';
        }
      }
    }
    function renderPrimaryResultCard(fact) {
      primaryResultEffectiveFact = fact;
      renderPrimaryResultCardSlots(buildPrimaryResultCardViewModelFromFact(fact));
      bindPrimaryResultInteractions();
    }
    async function loadPrimaryResultCard() {
      if (!PRIMARY_RESULT_BRIDGE_ENABLED) return;
      const serverFact = parseJsonScriptTag('primary-result-initial-json');
      if (!serverFact) return;
      primaryResultEffectiveFact = serverFact;
      bindPrimaryResultInteractions();
      const controller = new AbortController();
      const timeoutId = window.setTimeout(() => controller.abort(), 1500);
      try {
        const resp = await fetch(PRIMARY_RESULT_API_URL, {
          method: 'GET',
          headers: { 'Accept': 'application/json' },
          signal: controller.signal,
        });
        if (!resp.ok) return;
        const payload = await resp.json();
        if (!isValidPrimaryResultPayload(payload)) return;
        if (isDebugMode()) {
          const diff = comparePrimaryResultFacts(serverFact, payload);
          if (Object.keys(diff).length) {
            console.debug('primary-result fact mismatch', diff);
          }
        }
        const mergedFact = { ...serverFact, ...payload };
        renderPrimaryResultCard(mergedFact);
      } catch (error) {
        return;
      } finally {
        window.clearTimeout(timeoutId);
      }
    }
    async function loadTop5ManifestFreshnessBanner() {
      const mount = document.getElementById('top5-manifest-freshness-banner');
      if (!mount || !TOP5_MANIFEST_HEALTH_ENABLED || !TOP5_MANIFEST_HEALTH_URL) {
        return;
      }
      const controller = new AbortController();
      const timeoutId = window.setTimeout(() => controller.abort(), 2000);
      try {
        const resp = await fetch(TOP5_MANIFEST_HEALTH_URL, {
          method: 'GET',
          headers: { 'Accept': 'application/json' },
          signal: controller.signal,
        });
        if (!resp.ok) return;
        const data = await resp.json();
        if (!data || typeof data !== 'object') return;
        if (!data.eval_enabled || !data.stale_banner_recommended || !String(data.message_zh || '').trim()) {
          mount.setAttribute('hidden', 'hidden');
          return;
        }
        const cleaned = String(data.message_zh || '').replace(/\*\*([^*]+)\*\*/g, '$1');
        mount.textContent = cleaned;
        mount.removeAttribute('hidden');
      } catch (error) {
        return;
      } finally {
        window.clearTimeout(timeoutId);
      }
    }
    loadTop5ManifestFreshnessBanner();
