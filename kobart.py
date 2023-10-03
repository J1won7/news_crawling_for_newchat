import torch
from transformers import PreTrainedTokenizerFast
from transformers import BartForConditionalGeneration


class KoBart:
    def __init__(self):
        self.tokenizer = PreTrainedTokenizerFast.from_pretrained('gogamza/kobart-summarization')
        self.model = BartForConditionalGeneration.from_pretrained('gogamza/kobart-summarization').eval()

    def __call__(self, text):
        raw_input_ids = self.tokenizer.encode(text, max_length=512, truncation=True)
        input_ids = [self.tokenizer.bos_token_id] + raw_input_ids + [self.tokenizer.eos_token_id]
        summary_ids = self.model.generate(torch.tensor([input_ids]), num_beams=8, length_penalty=1.0, min_length=32,
                                          max_length=256)
        return self.tokenizer.decode(summary_ids.squeeze().tolist(), skip_special_tokens=True)
