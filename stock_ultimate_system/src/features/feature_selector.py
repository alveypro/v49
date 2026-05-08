class FeatureSelector:
    def select_by_correlation(self, df, target_col):
        corr = df.corr(numeric_only=True)[target_col].abs().sort_values(ascending=False)
        return [c for c in corr.index if c != target_col][:20]
