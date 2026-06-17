"""
ABSAModel — BERTimbau-large encoder + 5 classification heads.

Architecture:
  - Encoder: neuralmind/bert-large-portuguese-cased (hidden=1024)
  - Heads: overall + product_quality + delivery_speed + seller_service + price_value
  - Each head: Linear(1024, 3) → softmax → neg/neu/pos

Usage:
  model = ABSAModel.from_pretrained("neuralmind/bert-large-portuguese-cased")
  model = ABSAModel.load(weights_path, config_path)
"""
import json
import logging
from pathlib import Path

import torch
import torch.nn as nn
from transformers import BertModel, AutoTokenizer

log = logging.getLogger(__name__)

ASPECTS    = ["product_quality", "delivery_speed", "seller_service", "price_value"]
ALL_HEADS  = ["overall"] + ASPECTS
NUM_LABELS = 3
LABEL_NAMES = {0: "negative", 1: "neutral", 2: "positive"}


class ABSAModel(nn.Module):
    def __init__(self, pretrained: str, num_labels: int = NUM_LABELS, aspects: list[str] = ASPECTS):
        super().__init__()
        self.pretrained  = pretrained
        self.num_labels  = num_labels
        self.aspects     = aspects

        self.bert    = BertModel.from_pretrained(pretrained)
        hidden_size  = self.bert.config.hidden_size
        self.dropout = nn.Dropout(0.1)
        self.heads   = nn.ModuleDict({
            name: nn.Linear(hidden_size, num_labels)
            for name in (["overall"] + aspects)
        })

    def forward(
        self,
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> dict[str, torch.Tensor]:
        out    = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        pooled = self.dropout(out.pooler_output)
        return {name: head(pooled) for name, head in self.heads.items()}

    @classmethod
    def from_pretrained(cls, pretrained: str = "neuralmind/bert-large-portuguese-cased") -> "ABSAModel":
        log.info("Loading BERTimbau-large: %s", pretrained)
        return cls(pretrained=pretrained)

    def save(self, weights_path: str | Path, config_path: str | Path) -> None:
        torch.save(self.state_dict(), weights_path)
        config = {"pretrained": self.pretrained, "num_labels": self.num_labels, "aspects": self.aspects}
        Path(config_path).write_text(json.dumps(config, indent=2))
        log.info("Saved model → %s, config → %s", weights_path, config_path)

    @classmethod
    def load(cls, weights_path: str | Path, config_path: str | Path) -> "ABSAModel":
        config = json.loads(Path(config_path).read_text())
        model  = cls(
            pretrained  = config["pretrained"],
            num_labels  = config["num_labels"],
            aspects     = config["aspects"],
        )
        state = torch.load(weights_path, map_location="cpu", weights_only=True)
        model.load_state_dict(state)
        log.info("Loaded model weights from %s", weights_path)
        return model


def get_tokenizer(pretrained: str = "neuralmind/bert-large-portuguese-cased") -> AutoTokenizer:
    return AutoTokenizer.from_pretrained(pretrained)
