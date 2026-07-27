/// Randomly picks an index by weight. `roll` ∈ [0,1) is supplied by the caller to allow deterministic tests.
/// When the weights sum to 0 (or none are finite), it degrades to a uniform pick; an empty slice returns None.
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
    // Floating-point boundary fallback: return the last positive-weight index
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
        // weights [1.0, 3.0] → boundary 0.25
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
        // Sweep roll with a fixed step to verify the higher-weight upstream is chosen far more often
        let w = [1.0, 9.0];
        let mut counts = [0usize; 2];
        for i in 0..1000 {
            let roll = i as f64 / 1000.0;
            counts[pick_weighted(&w, roll).unwrap()] += 1;
        }
        assert!(
            counts[1] > counts[0] * 5,
            "9:1 weight should strongly favor index 1: {counts:?}"
        );
    }

    #[test]
    fn skips_nonfinite_and_negative_weights() {
        // NaN, negative, and infinite values should all be skipped: only indices 1 and 3 are valid positive weights
        let w = [f64::NAN, 1.0, -5.0, 3.0, f64::INFINITY];
        assert_eq!(pick_weighted(&w, 0.10), Some(1)); // 0.10*4=0.4 < 1.0
        assert_eq!(pick_weighted(&w, 0.30), Some(3)); // 0.30*4=1.2 ≥ 1.0
        assert_eq!(pick_weighted(&w, 0.99), Some(3));
    }

    #[test]
    fn single_negative_weight_degrades_to_uniform() {
        assert_eq!(pick_weighted(&[-5.0], 0.5), Some(0));
    }

    #[test]
    fn extreme_roll_hits_last_positive_fallback_safely() {
        // roll very close to 1 with wildly differing weight magnitudes; ensure no panic and a valid positive-weight index is returned
        let w = [1e-300, 1e300, 0.0];
        let idx = pick_weighted(&w, 0.999_999_999_999_999_9).unwrap();
        assert!(
            idx == 0 || idx == 1,
            "must land on a positive-weight index: {idx}"
        );
        // Boundary case: when roll exactly equals the first weight's share it falls into the next bucket (strict < semantics)
        let w2 = [1.0, 3.0];
        assert_eq!(pick_weighted(&w2, 0.25), Some(1));
    }
}
