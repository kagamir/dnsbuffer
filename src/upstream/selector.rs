/// 按权重随机抽取索引。roll ∈ [0,1) 由调用方提供以便确定性测试。
/// 权重总和为 0（或全部非有限）时退化为均匀抽取；空切片返回 None。
pub fn pick_weighted(weights: &[f64], roll: f64) -> Option<usize> {
    if weights.is_empty() {
        return None;
    }
    let total: f64 = weights.iter().filter(|w| w.is_finite() && **w > 0.0).sum();
    if total <= 0.0 {
        let idx = ((roll * weights.len() as f64) as usize).min(weights.len() - 1);
        return Some(idx);
    }
    let target = roll * total;
    let mut acc = 0.0;
    for (i, w) in weights.iter().enumerate() {
        if w.is_finite() && *w > 0.0 {
            acc += w;
            if target < acc {
                return Some(i);
            }
        }
    }
    // 浮点边界兜底：返回最后一个正权重索引
    weights.iter().rposition(|w| w.is_finite() && *w > 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_returns_none() {
        assert_eq!(pick_weighted(&[], 0.5), None);
    }

    #[test]
    fn single_always_picked() {
        assert_eq!(pick_weighted(&[0.7], 0.0), Some(0));
        assert_eq!(pick_weighted(&[0.7], 0.999), Some(0));
    }

    #[test]
    fn roll_lands_proportionally() {
        // weights [1.0, 3.0] → 边界 0.25
        let w = [1.0, 3.0];
        assert_eq!(pick_weighted(&w, 0.10), Some(0));
        assert_eq!(pick_weighted(&w, 0.24), Some(0));
        assert_eq!(pick_weighted(&w, 0.26), Some(1));
        assert_eq!(pick_weighted(&w, 0.90), Some(1));
    }

    #[test]
    fn zero_weights_degrade_to_uniform() {
        let w = [0.0, 0.0, 0.0, 0.0];
        assert_eq!(pick_weighted(&w, 0.0), Some(0));
        assert_eq!(pick_weighted(&w, 0.30), Some(1));
        assert_eq!(pick_weighted(&w, 0.99), Some(3));
    }

    #[test]
    fn statistical_bias_holds() {
        // 用固定步长扫 roll，验证高权重上游被选中次数显著更多
        let w = [1.0, 9.0];
        let mut counts = [0usize; 2];
        for i in 0..1000 {
            let roll = i as f64 / 1000.0;
            counts[pick_weighted(&w, roll).unwrap()] += 1;
        }
        assert!(counts[1] > counts[0] * 5, "9:1 权重应显著偏向索引 1: {counts:?}");
    }
}
