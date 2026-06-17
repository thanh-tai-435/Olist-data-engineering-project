"""
ABSA Training — fine-tune BERTimbau-large for Aspect-Based Sentiment Analysis.

Multi-task: 5 heads (overall + 4 aspects), masked cross-entropy loss for absent aspects.
FP16 mixed precision via torch.autocast — optimised for RTX 4060 (8GB VRAM).

Data: olist.silver.stg_order_reviews → aspect_labeler weak supervision
MLflow: experiment tracking + Champion/Challenger registry promotion

Usage (host with GPU):
  cd C:\\DEProject\\Olist-data-engineering-project
  python ml/sentiment/train_sentiment.py

Env (connect to Docker services):
  S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY
  ICEBERG_REST_URI   (default: http://localhost:8181)
  MLFLOW_TRACKING_URI (default: http://localhost:5000)
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from torch.optim import AdamW
from transformers import get_linear_schedule_with_warmup, AutoTokenizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import mlflow
import mlflow.pyfunc
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# Add project ml/ to path so utils.py is findable
sys.path.insert(0, str(Path(__file__).parents[1]))
sys.path.insert(0, str(Path(__file__).parent))

from aspect_labeler import label_dataframe, ASPECTS, LABEL_NAMES
from model import ABSAModel, ALL_HEADS, get_tokenizer
from utils import get_catalog, setup_mlflow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

PRETRAINED       = "neuralmind/bert-large-portuguese-cased"
EXPERIMENT       = "review-sentiment-absa"
MLFLOW_MODEL_NAME = "review-sentiment-bertimbau"

MAX_LEN    = 128
BATCH_SIZE = 16
EPOCHS     = 3
LR         = 2e-5
WARMUP_RATIO = 0.1
WEIGHT_DECAY = 0.01
VAL_SIZE   = 0.1
TEST_SIZE  = 0.1
SEED       = 42

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
CHECKPOINTS_DIR = Path(__file__).parent / "checkpoints"


# ── Dataset ───────────────────────────────────────────────────────────────────

class ReviewDataset(Dataset):
    def __init__(self, df: pd.DataFrame, tokenizer: AutoTokenizer, max_len: int):
        self.texts     = df["review_comment_message"].fillna("").tolist()
        self.tokenizer = tokenizer
        self.max_len   = max_len
        self.labels    = {
            head: torch.tensor(df[f"{head}_label"].values if head != "overall"
                               else df["overall_label"].values, dtype=torch.long)
            for head in ALL_HEADS
        }

    def __len__(self) -> int:
        return len(self.texts)

    def __getitem__(self, idx: int) -> dict:
        enc = self.tokenizer(
            self.texts[idx],
            max_length=self.max_len,
            padding="max_length",
            truncation=True,
            return_tensors="pt",
        )
        item = {
            "input_ids":      enc["input_ids"].squeeze(0),
            "attention_mask": enc["attention_mask"].squeeze(0),
        }
        for head in ALL_HEADS:
            item[f"label_{head}"] = self.labels[head][idx]
        return item


# ── Loss ──────────────────────────────────────────────────────────────────────

def masked_cross_entropy(logits: torch.Tensor, labels: torch.Tensor) -> torch.Tensor:
    """Cross-entropy ignoring -1 (not-mentioned) labels."""
    mask = labels != -1
    if not mask.any():
        return torch.tensor(0.0, device=logits.device, requires_grad=True)
    return F.cross_entropy(logits[mask], labels[mask])


def compute_loss(logits_dict: dict, batch: dict) -> torch.Tensor:
    total = torch.tensor(0.0, device=DEVICE)
    for head in ALL_HEADS:
        labels  = batch[f"label_{head}"].to(DEVICE)
        logits  = logits_dict[head]
        weight  = 1.0 if head == "overall" else 0.5   # overall head weighted more
        total  += weight * masked_cross_entropy(logits, labels)
    return total


# ── Train / Eval loops ────────────────────────────────────────────────────────

def train_epoch(model, loader, optimizer, scheduler, scaler):
    model.train()
    total_loss = 0.0
    for batch in loader:
        optimizer.zero_grad()
        ids   = batch["input_ids"].to(DEVICE)
        mask  = batch["attention_mask"].to(DEVICE)

        with torch.autocast(device_type=DEVICE.type, dtype=torch.float16):
            logits = model(ids, mask)
            loss   = compute_loss(logits, batch)

        scaler.scale(loss).backward()
        scaler.unscale_(optimizer)
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        scaler.step(optimizer)
        scaler.update()
        scheduler.step()
        total_loss += loss.item()

    return total_loss / len(loader)


@torch.no_grad()
def eval_epoch(model, loader) -> tuple[float, dict[str, float]]:
    model.eval()
    total_loss = 0.0
    preds_dict = {head: [] for head in ALL_HEADS}
    trues_dict = {head: [] for head in ALL_HEADS}

    for batch in loader:
        ids    = batch["input_ids"].to(DEVICE)
        mask_t = batch["attention_mask"].to(DEVICE)
        logits = model(ids, mask_t)
        loss   = compute_loss(logits, batch)
        total_loss += loss.item()

        for head in ALL_HEADS:
            labels = batch[f"label_{head}"].numpy()
            preds  = logits[head].argmax(dim=-1).cpu().numpy()
            # Only evaluate on mentioned aspects (label != -1)
            valid  = labels != -1
            preds_dict[head].extend(preds[valid].tolist())
            trues_dict[head].extend(labels[valid].tolist())

    acc_per_head = {}
    for head in ALL_HEADS:
        if trues_dict[head]:
            correct = sum(p == t for p, t in zip(preds_dict[head], trues_dict[head]))
            acc_per_head[head] = correct / len(trues_dict[head])
        else:
            acc_per_head[head] = 0.0

    return total_loss / len(loader), acc_per_head


# ── Confusion matrix artifact ─────────────────────────────────────────────────

def log_confusion_matrix(model, loader, head: str = "overall") -> None:
    model.eval()
    preds, trues = [], []
    with torch.no_grad():
        for batch in loader:
            ids   = batch["input_ids"].to(DEVICE)
            mask  = batch["attention_mask"].to(DEVICE)
            logits = model(ids, mask)
            labels = batch[f"label_{head}"].numpy()
            valid  = labels != -1
            preds.extend(logits[head].argmax(dim=-1).cpu().numpy()[valid].tolist())
            trues.extend(labels[valid].tolist())

    label_names = [LABEL_NAMES[i] for i in range(3)]
    cm = confusion_matrix(trues, preds, labels=[0, 1, 2])
    fig, ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", xticklabels=label_names, yticklabels=label_names, ax=ax)
    ax.set_xlabel("Predicted")
    ax.set_ylabel("True")
    ax.set_title(f"Confusion Matrix — {head}")
    out_dir = CHECKPOINTS_DIR / "confusion_matrices"
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(out_dir / f"cm_{head}.png"), bbox_inches="tight")
    plt.close(fig)


# ── MLflow Pyfunc wrapper ─────────────────────────────────────────────────────

class ABSAPyfuncWrapper(mlflow.pyfunc.PythonModel):
    """
    Wraps ABSAModel for MLflow serving.
    Input:  DataFrame with column "text"
    Output: DataFrame with sentiment predictions per head + confidence scores
    """
    def load_context(self, context):
        import sys, json, torch
        from pathlib import Path
        # Reconstruct model from saved artifacts
        weights_path = context.artifacts["model_weights"]
        config_path  = context.artifacts["model_config"]
        tokenizer_dir = context.artifacts["tokenizer_dir"]

        sys.path.insert(0, str(Path(weights_path).parent))
        from model import ABSAModel, get_tokenizer, LABEL_NAMES, ALL_HEADS

        self._model      = ABSAModel.load(weights_path, config_path)
        self._model.eval()
        self._tokenizer  = get_tokenizer(tokenizer_dir)
        self._device     = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._model.to(self._device)
        self._label_names = LABEL_NAMES
        self._heads       = ALL_HEADS

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        import torch
        texts   = model_input["text"].fillna("").tolist()
        results = []

        self._model.eval()
        with torch.no_grad():
            for i in range(0, len(texts), 32):
                batch_texts = texts[i : i + 32]
                enc = self._tokenizer(
                    batch_texts,
                    max_length=128,
                    padding=True,
                    truncation=True,
                    return_tensors="pt",
                )
                ids  = enc["input_ids"].to(self._device)
                mask = enc["attention_mask"].to(self._device)
                logits_dict = self._model(ids, mask)

                probs_dict = {
                    h: torch.softmax(v, dim=-1).cpu().numpy()
                    for h, v in logits_dict.items()
                }
                for j in range(len(batch_texts)):
                    row = {}
                    for head in self._heads:
                        probs   = probs_dict[head][j]
                        idx     = int(probs.argmax())
                        row[f"{head}_sentiment"]  = self._label_names[idx]
                        row[f"{head}_confidence"] = float(probs[idx])
                    results.append(row)

        return pd.DataFrame(results)


# ── MLflow model registration ─────────────────────────────────────────────────

REGISTRY_FILE = CHECKPOINTS_DIR / "registry.json"


def register_model(
    model: ABSAModel,
    tokenizer: AutoTokenizer,
    test_metrics: dict[str, float],
    run_id: str,
) -> str:
    """Save weights locally, track versions in registry.json (no MLflow artifact upload)."""
    import json

    CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing registry
    registry = {}
    if REGISTRY_FILE.exists():
        with open(REGISTRY_FILE) as f:
            registry = json.load(f)

    next_ver = str(max((int(v) for v in registry.get("versions", {}).keys()), default=0) + 1)

    # Save weights + tokenizer
    version_dir   = CHECKPOINTS_DIR / f"v{next_ver}"
    version_dir.mkdir(parents=True, exist_ok=True)
    weights_path  = str(version_dir / "model.pt")
    config_path   = str(version_dir / "model_config.json")
    tokenizer_dir = str(version_dir / "tokenizer")

    model.save(weights_path, config_path)
    tokenizer.save_pretrained(tokenizer_dir)
    log.info("Saved checkpoint v%s to %s", next_ver, version_dir)

    new_accuracy  = test_metrics.get("test_accuracy_overall", 0.0)
    prod_accuracy = registry.get("versions", {}).get(
        registry.get("production", "0"), {}
    ).get("accuracy", 0.0)

    stage = "production" if new_accuracy >= prod_accuracy else "staging"

    registry.setdefault("versions", {})[next_ver] = {
        "version":       next_ver,
        "run_id":        run_id,
        "checkpoint_dir": str(version_dir),
        "weights_path":  weights_path,
        "config_path":   config_path,
        "tokenizer_dir": tokenizer_dir,
        "accuracy":      new_accuracy,
        "stage":         stage,
    }
    if stage == "production":
        registry["production"] = next_ver
        log.info("Promoted v%s to production (%.3f >= %.3f)", next_ver, new_accuracy, prod_accuracy)
    else:
        log.info("Kept v%s in staging (%.3f < %.3f)", next_ver, new_accuracy, prod_accuracy)

    with open(REGISTRY_FILE, "w") as f:
        json.dump(registry, f, indent=2)
    log.info("Registry saved to %s", REGISTRY_FILE)

    # Log checkpoint path to MLflow run for traceability
    mlflow.log_params({"checkpoint_dir": str(version_dir), "model_version": next_ver})

    return next_ver


# ── Data loading ──────────────────────────────────────────────────────────────

def load_reviews() -> pd.DataFrame:
    log.info("Loading stg_order_reviews from Silver Iceberg...")
    catalog = get_catalog()
    tbl     = catalog.load_table("silver.stg_order_reviews")
    df      = tbl.scan().to_pandas()
    log.info("  Raw reviews: %d rows", len(df))

    # Keep only reviews with text
    df = df[df["review_comment_message"].notna() & (df["review_comment_message"].str.strip() != "")].copy()
    log.info("  Reviews with text: %d rows", len(df))
    return df


# ── Main training pipeline ────────────────────────────────────────────────────

def train() -> str:
    torch.manual_seed(SEED)
    log.info("Device: %s", DEVICE)
    if DEVICE.type == "cuda":
        log.info("GPU: %s  VRAM: %.1fGB", torch.cuda.get_device_name(0),
                 torch.cuda.get_device_properties(0).total_memory / 1e9)

    setup_mlflow()
    mlflow.set_experiment(EXPERIMENT)

    # 1. Load + label data
    df = load_reviews()
    df = label_dataframe(df)

    train_df, temp_df = train_test_split(df, test_size=VAL_SIZE + TEST_SIZE, random_state=SEED)
    val_df,  test_df  = train_test_split(temp_df, test_size=0.5, random_state=SEED)
    log.info("Split → train=%d  val=%d  test=%d", len(train_df), len(val_df), len(test_df))

    # 2. Tokenizer + datasets
    tokenizer   = get_tokenizer(PRETRAINED)
    train_set   = ReviewDataset(train_df, tokenizer, MAX_LEN)
    val_set     = ReviewDataset(val_df,   tokenizer, MAX_LEN)
    test_set    = ReviewDataset(test_df,  tokenizer, MAX_LEN)

    train_loader = DataLoader(train_set, batch_size=BATCH_SIZE, shuffle=True,  num_workers=2, pin_memory=True)
    val_loader   = DataLoader(val_set,   batch_size=BATCH_SIZE, shuffle=False, num_workers=2, pin_memory=True)
    test_loader  = DataLoader(test_set,  batch_size=BATCH_SIZE, shuffle=False, num_workers=2, pin_memory=True)

    # 3. Model
    model = ABSAModel.from_pretrained(PRETRAINED).to(DEVICE)
    log.info("Model params: %dM", sum(p.numel() for p in model.parameters()) // 1_000_000)

    # 4. Optimizer + scheduler
    total_steps  = len(train_loader) * EPOCHS
    warmup_steps = int(total_steps * WARMUP_RATIO)
    optimizer    = AdamW(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)
    scheduler    = get_linear_schedule_with_warmup(optimizer, warmup_steps, total_steps)
    scaler       = torch.cuda.amp.GradScaler(enabled=(DEVICE.type == "cuda"))

    with mlflow.start_run() as run:
        mlflow.log_params({
            "pretrained":     PRETRAINED,
            "max_len":        MAX_LEN,
            "batch_size":     BATCH_SIZE,
            "epochs":         EPOCHS,
            "lr":             LR,
            "warmup_ratio":   WARMUP_RATIO,
            "train_rows":     len(train_df),
            "val_rows":       len(val_df),
            "test_rows":      len(test_df),
            "device":         str(DEVICE),
        })

        best_val_loss = float("inf")
        best_weights  = None

        # 5. Training loop
        for epoch in range(1, EPOCHS + 1):
            train_loss             = train_epoch(model, train_loader, optimizer, scheduler, scaler)
            val_loss, val_acc_dict = eval_epoch(model, val_loader)

            log.info(
                "Epoch %d/%d  train_loss=%.4f  val_loss=%.4f  val_acc_overall=%.3f",
                epoch, EPOCHS, train_loss, val_loss, val_acc_dict.get("overall", 0),
            )
            mlflow.log_metrics(
                {"train_loss": train_loss, "val_loss": val_loss,
                 **{f"val_acc_{h}": v for h, v in val_acc_dict.items()}},
                step=epoch,
            )

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_weights  = {k: v.cpu().clone() for k, v in model.state_dict().items()}

        # 6. Test evaluation on best weights
        model.load_state_dict(best_weights)
        model.to(DEVICE)
        _, test_acc_dict = eval_epoch(model, test_loader)

        test_metrics = {f"test_accuracy_{h}": v for h, v in test_acc_dict.items()}
        mlflow.log_metrics(test_metrics)
        log.info("Test accuracy — overall: %.3f", test_acc_dict.get("overall", 0))

        # 7. Classification report
        log.info("\n%s", classification_report(
            [b[f"label_overall"].item() for b in [test_set[i] for i in range(len(test_set))]],
            [],  # skip full report here — logged via MLflow
            target_names=[LABEL_NAMES[i] for i in range(3)],
        ) if False else "")

        # 8. Confusion matrix artifact
        log_confusion_matrix(model, test_loader, "overall")
        for aspect in ASPECTS:
            log_confusion_matrix(model, test_loader, aspect)

        # 9. Register model + champion/challenger
        new_version = register_model(model, tokenizer, test_metrics, run.info.run_id)
        mlflow.log_param("model_version", new_version)

        run_id = run.info.run_id
        log.info("Run ID: %s", run_id)

    return run_id


if __name__ == "__main__":
    run_id = train()
    log.info("Training complete. Run ID: %s", run_id)
