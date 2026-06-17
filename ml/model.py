"""
ML 모델 학습/예측 모듈
scikit-learn 없이 순수 Python으로 구현
(Railway 환경에서 scikit-learn 설치 없이 동작)

추후 scikit-learn 추가 시 RandomForest로 교체 가능
"""
import json
import math
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


# ── 간단한 나이브 베이즈 분류기 ────────────────────────
class SimpleNaiveBayes:
    """
    가우시안 나이브 베이즈 분류기
    scikit-learn 없이 순수 Python 구현
    """

    def __init__(self):
        self.classes = []
        self.class_priors = {}
        self.feature_stats = {}  # {class: {feature: (mean, std)}}
        self.feature_names = []
        self.is_trained = False

    def fit(self, X: List[List[float]], y: List[int], feature_names: List[str] = None):
        """모델 학습"""
        if not X or not y:
            return self

        self.feature_names = feature_names or [f"f{i}" for i in range(len(X[0]))]
        self.classes = list(set(y))

        for cls in self.classes:
            # 해당 클래스 샘플
            cls_samples = [X[i] for i in range(len(y)) if y[i] == cls]
            self.class_priors[cls] = len(cls_samples) / len(y)

            # 피처별 평균/표준편차
            self.feature_stats[cls] = {}
            n_features = len(X[0])
            for j in range(n_features):
                values = [s[j] for s in cls_samples]
                mean = sum(values) / len(values)
                variance = sum((v - mean) ** 2 for v in values) / len(values)
                std = math.sqrt(variance) if variance > 0 else 1e-6
                feat_name = self.feature_names[j] if j < len(self.feature_names) else f"f{j}"
                self.feature_stats[cls][feat_name] = (mean, std)

        self.is_trained = True
        logger.info(f"✅ 모델 학습 완료 (샘플: {len(X)}, 클래스: {self.classes})")
        return self

    def _gaussian_prob(self, x: float, mean: float, std: float) -> float:
        """가우시안 확률 밀도"""
        exponent = -((x - mean) ** 2) / (2 * std ** 2)
        return (1 / (math.sqrt(2 * math.pi) * std)) * math.exp(exponent)

    def predict_proba(self, x: List[float]) -> Dict[int, float]:
        """클래스별 확률 반환"""
        if not self.is_trained:
            return {0: 0.5, 1: 0.5}

        log_probs = {}
        for cls in self.classes:
            log_prob = math.log(self.class_priors[cls])
            for j, feat_name in enumerate(self.feature_names):
                if feat_name in self.feature_stats[cls] and j < len(x):
                    mean, std = self.feature_stats[cls][feat_name]
                    prob = self._gaussian_prob(x[j], mean, std)
                    log_prob += math.log(max(prob, 1e-300))
            log_probs[cls] = log_prob

        # 소프트맥스
        max_log = max(log_probs.values())
        exp_probs = {cls: math.exp(lp - max_log) for cls, lp in log_probs.items()}
        total = sum(exp_probs.values())
        return {cls: p / total for cls, p in exp_probs.items()}

    def predict(self, x: List[float]) -> int:
        """예측 클래스 반환"""
        probs = self.predict_proba(x)
        return max(probs, key=probs.get)

    def to_dict(self) -> Dict:
        """모델 직렬화"""
        return {
            "classes": self.classes,
            "class_priors": self.class_priors,
            "feature_stats": self.feature_stats,
            "feature_names": self.feature_names,
            "is_trained": self.is_trained,
        }

    def from_dict(self, d: Dict):
        """모델 역직렬화"""
        self.classes = d["classes"]
        self.class_priors = d["class_priors"]
        self.feature_stats = d["feature_stats"]
        self.feature_names = d["feature_names"]
        self.is_trained = d["is_trained"]
        return self


# ── 모델 관리자 ─────────────────────────────────────────
class MLModelManager:
    """ML 모델 학습/예측/저장 관리"""

    def __init__(self, db_pool=None):
        self.db_pool = db_pool
        self.models = {}  # {symbol: SimpleNaiveBayes}

    async def train(self, symbol: str, ohlcv: List[Dict]) -> Dict:
        """종목별 모델 학습"""
        from ml.features import build_features, get_feature_names

        features = build_features(ohlcv, lookback=60)
        if len(features) < 50:
            return {"success": False, "error": f"학습 데이터 부족 ({len(features)}개, 최소 50개 필요)"}

        feat_names = get_feature_names()
        X = [[f[name] for name in feat_names] for f in features]
        y = [f["label_buy"] for f in features]

        # 클래스 불균형 확인
        n_buy = sum(y)
        n_sell = len(y) - n_buy
        logger.info(f"[{symbol}] 학습 데이터: 총 {len(y)}개 (매수 {n_buy}, 매도 {n_sell})")

        model = SimpleNaiveBayes()
        model.fit(X, y, feat_names)
        self.models[symbol] = model

        # 간단한 교차 검증 (80/20 split)
        split = int(len(X) * 0.8)
        train_X, test_X = X[:split], X[split:]
        train_y, test_y = y[:split], y[split:]

        model_test = SimpleNaiveBayes()
        model_test.fit(train_X, train_y, feat_names)

        correct = sum(1 for i in range(len(test_X))
                      if model_test.predict(test_X[i]) == test_y[i])
        accuracy = correct / len(test_X) if test_X else 0

        # 모델 저장
        await self._save_model(symbol, model, accuracy=round(accuracy * 100, 1), samples=len(features))

        return {
            "success": True,
            "symbol": symbol,
            "samples": len(features),
            "accuracy": round(accuracy * 100, 1),
            "buy_ratio": round(n_buy / len(y) * 100, 1),
            "feature_count": len(feat_names),
        }

    async def predict(self, symbol: str, ohlcv: List[Dict]) -> Dict:
        """현재 시점 매수/매도 예측"""
        # 모델 로드
        if symbol not in self.models:
            model = await self._load_model(symbol)
            if model is None:
                return {"success": False, "error": "학습된 모델 없음"}
            self.models[symbol] = model

        model = self.models[symbol]

        from ml.features import build_features, get_feature_names
        features = build_features(ohlcv, lookback=60)
        if not features:
            return {"success": False, "error": "피처 생성 실패"}

        feat_names = get_feature_names()
        latest = features[-1]
        x = [latest[name] for name in feat_names]

        proba = model.predict_proba(x)
        buy_prob = proba.get(1, 0.5)
        sell_prob = proba.get(0, 0.5)

        # 신호 결정
        if buy_prob >= 0.65:
            signal = "BUY"
        elif buy_prob <= 0.35:
            signal = "SELL"
        else:
            signal = "HOLD"

        result = {
            "success": True,
            "symbol": symbol,
            "ts": datetime.now().isoformat(),
            "buy_prob": round(buy_prob, 4),
            "sell_prob": round(sell_prob, 4),
            "signal": signal,
            "confidence": round(abs(buy_prob - 0.5) * 200, 1),
            "top_features": self._get_top_features(latest, feat_names),
        }

        # DB에 예측 저장
        await self._save_prediction(symbol, result)
        return result

    def _get_top_features(self, features: Dict, feat_names: List[str]) -> List[Dict]:
        """영향력 높은 피처 반환"""
        important = [
            {"name": "RSI", "value": round(features.get("rsi", 50), 1),
             "signal": "과매도" if features.get("rsi", 50) < 30 else ("과매수" if features.get("rsi", 50) > 70 else "정상")},
            {"name": "MACD", "value": round(features.get("macd_hist", 0), 0),
             "signal": "상승" if features.get("macd_hist", 0) > 0 else "하락"},
            {"name": "볼린저", "value": round(features.get("bb_pct", 0.5), 2),
             "signal": "과매도" if features.get("bb_pct", 0.5) < 0.2 else ("과매수" if features.get("bb_pct", 0.5) > 0.8 else "정상")},
            {"name": "골든크로스", "value": features.get("golden_cross", 0),
             "signal": "발생" if features.get("golden_cross", 0) else "없음"},
        ]
        return important

    async def _save_model(self, symbol: str, model: SimpleNaiveBayes, accuracy: float = 0, samples: int = 0):
        """모델을 DB에 저장"""
        if not self.db_pool:
            return
        try:
            model_json = json.dumps(model.to_dict())
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO ml_models (symbol, model_name, model_data, accuracy, samples, updated_at)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    ON CONFLICT (symbol, model_name) DO UPDATE
                    SET model_data=$3, accuracy=$4, samples=$5, updated_at=NOW()
                """, symbol, "naive_bayes", model_json, accuracy, samples)
            logger.info(f"✅ 모델 저장: {symbol}")
        except Exception as e:
            logger.error(f"모델 저장 실패: {e}")

    async def _load_model(self, symbol: str) -> Optional[SimpleNaiveBayes]:
        """DB에서 모델 로드"""
        if not self.db_pool:
            return None
        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT model_data FROM ml_models WHERE symbol=$1 AND model_name='naive_bayes'",
                    symbol
                )
            if row:
                model_data = json.loads(row["model_data"])
                model = SimpleNaiveBayes()
                model.from_dict(model_data)
                logger.info(f"✅ 모델 로드: {symbol}")
                return model
        except Exception as e:
            logger.error(f"모델 로드 실패: {e}")
        return None

    async def _save_prediction(self, symbol: str, result: Dict):
        """예측 결과 DB 저장"""
        if not self.db_pool:
            return
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO ml_predictions
                        (symbol, ts, model_name, buy_prob, sell_prob, signal, features)
                    VALUES ($1, NOW(), $2, $3, $4, $5, $6)
                """, symbol, "naive_bayes",
                    result["buy_prob"], result["sell_prob"],
                    result["signal"],
                    json.dumps(result.get("top_features", [])))
        except Exception as e:
            logger.error(f"예측 저장 실패: {e}")

    async def get_all_predictions(self) -> List[Dict]:
        """모든 종목 최신 예측 조회 (accuracy 포함)"""
        if not self.db_pool:
            return []
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT DISTINCT ON (p.symbol)
                        p.symbol, p.ts, p.buy_prob, p.sell_prob, p.signal,
                        m.accuracy, m.samples
                    FROM ml_predictions p
                    LEFT JOIN ml_models m ON p.symbol = m.symbol AND m.model_name='naive_bayes'
                    ORDER BY p.symbol, p.ts DESC
                """)
            result = []
            for r in rows:
                d = dict(r)
                if d.get("ts"):
                    d["ts"] = d["ts"].isoformat()
                result.append(d)
            return result
        except Exception as e:
            logger.error(f"예측 조회 실패: {e}")
            return []
