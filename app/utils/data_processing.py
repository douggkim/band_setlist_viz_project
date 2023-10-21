import re 

def camel_to_snake(text:str) -> str:
    """transform the given text to snake case while replacing . with _s. For processing column names. 

    Args:
        text (str): string to process 

    Returns:
        str: transformed string
    """
    # Replace '.' with ' ' (space)
    text = text.replace('.', '_')
    # Use regular expression to convert camel case to snake case
    text = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', text)
    
    return text.lower()