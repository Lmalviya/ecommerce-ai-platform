import re
import html
from typing import List
from src.models import ProductDraft

class ProductCleaner:
    """
    Stage 2: Cleaning
    Removes HTML, fixes whitespace, and standardizes text fields.
    """

    def __init__(self):
        # Regex to match any HTML tag like <div> or <br/>
        self.html_re = re.compile('<.*?>')

    def clean_text(self, text: str) -> str:
        """Helper to remove HTML and fix spaces."""
        if not text:
            return ""
        
        # 1. Remove HTML tags
        text = re.sub(self.html_re, '', text)
        
        # 2. Convert &amp; to &
        text = html.unescape(text)
        
        # 3. Fix whitespace (remove newlines and multiple spaces)
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = ' '.join(text.split())
        
        return text.strip()

    def process(self, draft: ProductDraft) -> ProductDraft:
        """Main method to clean a single product draft."""
        
        # Clean basic strings
        draft.title = self.clean_text(draft.title)
        draft.description = self.clean_text(draft.description)
        draft.brand = self.clean_text(draft.brand)
        draft.seller_name = self.clean_text(draft.seller_name)
        
        # Standardize categories
        if draft.categories:
            draft.categories = [self.clean_text(c) for c in draft.categories if c]
            
        # Standardize features (ensure no empty strings or junk)
        if draft.features:
            draft.features = [self.clean_text(f) for f in draft.features if f]

        return draft
