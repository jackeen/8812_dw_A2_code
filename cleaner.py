import ollama
import json
import re

def parse_reviews_by_llm(input):
    prompt = f"""
    You are a professional data-cleaning assistant.

    Parse the following semi-structured game review text into a JSON array.
    Each review follows the pattern: "quote" source (source may contain a score numbers like 9/10).

    Requirements:
    1. Output **pure standard JSON only**, no explanations, no markdown, no code blocks.
    2. Each object must have three fields:
        - "quote": the text inside the quotes (string)
        - "source": the publisher or website name (string)
        - "rating": the numeric score if present (e.g., "9", "8.5"), otherwise null
        - "out_of": the numeric full score
    3. Extract ratings using patterns like: X/10, X.5/10, X out of 10, X-Y/10, etc.
    4. Preserve original order. Do not skip or reorder entries.
    5. If quotes are missing or malformed, infer boundaries using semantics.
    6. Trim extra whitespace from all fields.
    7. If multiple scores appear, use the first one.
    8. If the input can not be converted into required data structure return an empty string with no explaining.

    Text:
    \"\"\"{input}\"\"\"
    """
    return llm_parse(prompt)


def parse_languages_by_llm(input):
    pass


def parse_languages(input):
    normal_map = {
        "Spain": "Spanish",
        "Traditional": "Chinese",
        "Simplified": "Chinese",
        "Brazil": "Portuguese",
        "Gurmukhi": "Punjabi",
    }
    matches = re.findall(r"[A-Z][a-z]+", input)
    lang_set = set()
    for lang in matches:
        norm_name = normal_map.get(lang, "")
        if norm_name != "":
            lang = norm_name
        lang_set.add(lang)
        
    return lang_set




def llm_parse(prompt):
    response = ollama.chat(model="granite3.3:8b", messages=[
        {"role": "user", "content": prompt}
    ])

    raw = response["message"]["content"]
    final_json = []
    try:
        final_json = json.loads(raw)
    except json.JSONDecodeError:
        # print("llm convert failed")
        pass

    return final_json


def normalize_quotes(text: str) -> str:
    """
    convert all types of curly quotation marks into standard English quotation marks.
    """
    quote_map = {
        '‘': "'",
        '’': "'",
        '“': '"',
        '”': '"',
        '´': "'",
        '′': "'",
        '‛': "'",
        '″': '"',
        '〝': '"',
        '〞': '"',
        '＇': "'",  
        '＂': '"',
        #'–': '-',
    }

    pattern = re.compile('|'.join(re.escape(k) for k in quote_map.keys()))
    return pattern.sub(lambda m: quote_map[m.group()], text)



def test():
    review = """
    “It’s like working at McDonalds in the middle of a tornado while demonic versions of Ronald McDonald and the Hamburglar try to sabotage your every move” The Game Center “You might have guessed from the grime that Cooking Mama this is not” PC Gamer “Happy’s Humble Burger Farm is a genius game” 9/10 - Superb – Destructoid
    """

    print(f"Start convert a review: {review}\n")
    res = parse_reviews_by_llm(normalize_quotes(review))
    print(f"The review is formatted as: \n{json.dumps(res)}\n")

    print(f"Start convert a empty review")
    res = parse_reviews_by_llm("")
    print(f"The review is formatted as: {res}\n")

    bad_review = "abcd"
    print(f"Start convert a bad review: {bad_review}")
    res = parse_reviews_by_llm(bad_review)
    print(f"The review is formatted as: {res}\n")



if __name__ == "__main__":
    
    test()
    
    

