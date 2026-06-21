"""
Weak supervision: aspect labels từ Portuguese keyword matching + review_score.

Label encoding:
  -1 = aspect not mentioned (masked in training loss)
   0 = negative
   1 = neutral
   2 = positive

Aspects: product_quality, delivery_speed, seller_service, price_value
"""
import logging
import re

import pandas as pd

log = logging.getLogger(__name__)

SENTIMENT_MAP = {1: 0, 2: 0, 3: 1, 4: 2, 5: 2}
LABEL_NAMES   = {0: "negative", 1: "neutral", 2: "positive"}
ASPECTS       = ["product_quality", "delivery_speed", "seller_service", "price_value"]

ASPECT_KEYWORDS: dict[str, list[str]] = {
    "product_quality": [
        "qualidade", "produto", "item", "material", "resistente", "durável",
        "robusto", "lindo", "bonito", "perfeito", "defeito", "quebrou",
        "falhou", "fraco", "ruim", "péssimo", "horrível", "rasgo",
        "manchado", "estragado", "danificado", "acabamento", "textura",
    ],
    "delivery_speed": [
        "entrega", "entregou", "chegou", "prazo", "tempo", "dias",
        "rápido", "rápida", "veloz", "atrasou", "atraso", "demorou",
        "demorada", "demora", "pontual", "transportadora", "correios",
        "logística", "frete", "postagem",
    ],
    "seller_service": [
        "vendedor", "loja", "atendimento", "resposta", "respondeu",
        "comunicação", "suporte", "contato", "devolução", "troca",
        "reembolso", "reclamação", "problema", "solução", "serviço",
    ],
    "price_value": [
        "preço", "valor", "custo", "caro", "barato", "promoção",
        "desconto", "cobrou", "pagamento", "vale", "econômico",
        "custa", "pago",
    ],
}

_PATTERNS = {
    aspect: re.compile(
        r"(" + "|".join(re.escape(kw) for kw in kws) + r")",
        re.IGNORECASE,
    )
    for aspect, kws in ASPECT_KEYWORDS.items()
}


def score_to_label(score) -> int:
    return SENTIMENT_MAP.get(int(score), 1)


def label_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add overall_label and per-aspect {aspect}_label / {aspect}_mentioned columns.
    Input df must have: review_comment_message (str), review_score (int).
    Rows with null text get -1 for all aspect labels (excluded from aspect losses).
    """
    df = df.copy()
    df["overall_label"] = df["review_score"].apply(score_to_label)

    for aspect in ASPECTS:
        pat = _PATTERNS[aspect]
        mentioned = df["review_comment_message"].apply(
            lambda t, p=pat: bool(p.search(str(t))) if pd.notna(t) and str(t).strip() else False
        )
        df[f"{aspect}_mentioned"] = mentioned
        df[f"{aspect}_label"] = df.apply(
            lambda r, a=aspect: score_to_label(r["review_score"]) if r[f"{a}_mentioned"] else -1,
            axis=1,
        )

    coverage = {a: df[f"{a}_mentioned"].mean() for a in ASPECTS}
    log.info("Aspect keyword coverage: %s", {k: f"{v:.1%}" for k, v in coverage.items()})
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sample = pd.DataFrame({
        "review_comment_message": [
            "O produto chegou rápido mas a qualidade era péssima",
            "Ótimo atendimento do vendedor, entrega no prazo!",
            "Produto ok, preço justo",
            "Defeito no item, demorou demais para chegar",
            None,
        ],
        "review_score": [2, 5, 4, 1, 3],
    })
    result = label_dataframe(sample)
    cols = ["overall_label"] + [f"{a}_label" for a in ASPECTS] + [f"{a}_mentioned" for a in ASPECTS]
    print(result[cols].to_string())
