"""Quick smoke test for post-training steps — runs in ~30s without full training."""
import sys
import torch
import mlflow
import matplotlib
matplotlib.use("Agg")

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[1]))
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from model import ABSAModel, ASPECTS, get_tokenizer
from utils import setup_mlflow
from train_sentiment import log_confusion_matrix, register_model, DEVICE

setup_mlflow()
mlflow.set_experiment("review-sentiment-absa")

print("Loading model...")
model = ABSAModel.from_pretrained("neuralmind/bert-large-portuguese-cased").to(DEVICE)
tokenizer = get_tokenizer("neuralmind/bert-large-portuguese-cased")
print("Model loaded.")


class DummyBatch:
    def __iter__(self):
        for _ in range(2):
            b = {
                "input_ids":      torch.zeros(4, 32, dtype=torch.long).to(DEVICE),
                "attention_mask": torch.ones(4, 32, dtype=torch.long).to(DEVICE),
                "label_overall":  torch.tensor([0, 1, 2, 1]),
            }
            for asp in ASPECTS:
                b[f"label_{asp}"] = torch.full((4,), -1)
            yield b


with mlflow.start_run() as run:
    log_confusion_matrix(model, DummyBatch(), "overall")
    print("log_confusion_matrix: OK")

    ver = register_model(model, tokenizer, {"test_accuracy_overall": 0.868}, run.info.run_id)
    print(f"register_model: OK  version={ver}")

print("ALL POST-TRAINING STEPS PASSED")
