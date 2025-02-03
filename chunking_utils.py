import logging
import json
import os
import hashlib
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from pypdf import PdfReader, PdfWriter
from io import BytesIO
import requests
import re

import tiktoken

def split_text(text, chunk_size, overlap, tokenizer=None, segment_size=5):
    if tokenizer is None:
        tokenizer = tiktoken.get_encoding("cl100k_base")
        
    words = text.split(' ')
    chunks = []
    i = 0
    while i < len(words):
        # Accumulate words until the encoded token count reaches or exceeds chunk_size
        current_chunk = []
        token_count = 0
        while token_count < chunk_size and i < len(words):
            current_chunk.append(words[i])
            temp_chunk = ' '.join(current_chunk)
            if i%segment_size == 0:
                tokens = tokenizer.encode(temp_chunk)
                token_count = len(tokens)
            i += 1  # Move to the next word

        # When the chunk reaches the desired token count, store it
        chunk = ' '.join(current_chunk)
        start_index = max(0, i - len(current_chunk))  # Calculate the start index of the chunk
        end_index = i  # This is the current position in the word list
        chunks.append((start_index, end_index, chunk))

        # Prepare for the next chunk
        # Overlap by going back 'overlap' tokens in the chunk, not words
        if overlap > 0 and i < len(words):
            # Find how many words correspond to 'overlap' tokens
            overlap_tokens = tokenizer.encode(' '.join(current_chunk[-overlap:]))
            if len(overlap_tokens) < overlap:
                overlap_count = len(current_chunk[-overlap:])
            else:
                overlap_count = sum(len(tokenizer.encode(' '.join(current_chunk[-j:]))) <= overlap for j in range(1, len(current_chunk) + 1))
            i = end_index - overlap_count  # Adjust index to account for overlap in tokens

    return chunks

def create_chunks(chunks_content_dict, chunk_size, overlap):

    chunks_content_dict = dict(sorted(chunks_content_dict.items()))

    pages = chunks_content_dict

    full_text = ""
    page_boundaries = {}
    current_length = 0

    # Initialize tokenizer
    enc = tiktoken.get_encoding("cl100k_base")

    for page_number, text in pages.items():
        text = text['content']
        tokens = text.split(' ')
        page_boundaries[page_number] = (current_length, current_length + len(tokens))
        full_text += text + " "  # Add a space to separate pages
        current_length += len(tokens)

    # Now split the full text
    chunks = split_text(full_text, chunk_size, overlap, enc)

    # Determine the page number for each chunk
    chunk_page_map = []

    for start, end, chunk in chunks:
        pages_involved = {page: bounds for page, bounds in page_boundaries.items() if start < bounds[1] and end >= bounds[0]}
        chunk_page_map.append((pages_involved, chunk))

    processed_chunks = []

    # Displaying results
    for pages, chunk in chunk_page_map:
        min_page = min(pages.keys())
        max_page = max(pages.keys())
        middle_page = (min_page + max_page) // 2

        source_record = chunks_content_dict[middle_page].copy()
        source_record['content'] = chunk
        try:
            del source_record['embeddings']
        except Exception as e:
            pass

        source_record['firstpage'] = min_page
        source_record['lastpage'] = max_page
        source_record['pagenumber'] = middle_page

        id_str = chunk + str(min_page) + str(max_page) + str(middle_page)
        hash_object = hashlib.sha256()
        hash_object.update(id_str.encode('utf-8'))
        id = hash_object.hexdigest()
        source_record['id'] = id


        processed_chunks.append(source_record)
    
    return processed_chunks


#######################################################################
#                      Semantic Chunking Utilities
#######################################################################

import mistune
import re

# A simple Node class to represent sections of the Markdown document.
class Node:
    def __init__(self, header=None, level=0):
        self.header = header      # The header text (if any)
        self.level = level        # Heading level (e.g., 1 for #, 2 for ##, etc.)
        self.content = []         # Non-heading tokens (or text) belonging to this node
        self.children = []        # Child nodes representing subsections

    def __repr__(self):
        return f"Node(header={self.header!r}, level={self.level}, children={self.children})"
    
def build_tree_from_ast(ast_tokens):
    root = Node(header="root", level=0)
    stack = [root]

    for token in ast_tokens:
        if token.get("type") == "heading":
            level = token.get("level") or token.get("attrs", {}).get("level", 1)
            header_text = "".join(child.get("text", "") for child in token.get("children", []))
            new_node = Node(header=header_text, level=level)
            while stack and stack[-1].level >= level:
                stack.pop()
            stack[-1].children.append(new_node)
            stack.append(new_node)
        else:
            stack[-1].content.append(token)
    
    # Optional: if root has content, wrap it in an "Introduction" node.
    if root.content:
        intro = Node(header="Introduction", level=1)
        intro.content = root.content
        root.children.insert(0, intro)
        root.content = []  # Clear the root's own content

    return root

import tiktoken

def token_count(text):
    # Initialize the tokenizer
    encoding = tiktoken.get_encoding("cl100k_base")
    
    # Encode the text to get the tokens
    tokens = encoding.encode(text)
    
    # Return the number of tokens
    return len(tokens)


def split_string(string, n):
    """
    Split a string into n parts.
    
    Parameters:
      string (str): The string to be split.
      n (int): The number of parts to split the string into.
    
    Returns:
      list: A list containing the split parts of the string.
    """
    # Calculate the length of each part
    part_length = len(string) // n
    # Calculate the number of characters left after equal division
    remainder = len(string) % n

    parts = []
    start = 0

    for i in range(n):
        # Calculate the end index for the current part
        end = start + part_length + (1 if i < remainder else 0)
        # Append the current part to the list
        parts.append(string[start:end])
        # Update the start index for the next part
        start = end

    return parts

def split_node(node_string, token_limit):
    """
    Splits a node (represented as a string) into smaller pieces that
    are within the token limit.
    
    Parameters:
      node_string (str): The string to be split.
      token_limit (int): The maximum token count allowed per piece.
      
    Returns:
      list: A list of string pieces.
    """
    sub_nodes = []
    parts = 2
    while True:
        curr_parts = split_string(node_string, parts)
        if token_count(curr_parts[0]) <= token_limit:
            sub_nodes = curr_parts
            break
        parts += 1
    return sub_nodes

# === Helper Functions to Render and Process the AST ===

def render_node(node):
    """
    Recursively renders an AST node (a dictionary) into a markdown string.
    
    This function looks at the node type:
      - For type 'text': returns the node's 'raw' text.
      - For type 'softbreak': returns a newline.
      - For type 'blank_line': returns a newline.
      - For a 'paragraph': renders its children and appends a newline.
      - For a 'heading': prepends '#' characters based on its level (from attrs)
        and then renders its children.
      - For 'block_html': returns the raw HTML.
      - Otherwise, if children exist, joins their rendered text.
    """
    t = node.get('type')
    if t == 'text':
        return node.get('raw', '')
    elif t == 'softbreak':
        return '\n'
    elif t == 'blank_line':
        return '\n'
    elif t == 'paragraph':
        children = node.get('children', [])
        rendered = ''.join(render_node(child) for child in children)
        return rendered + '\n'
    elif t == 'heading':
        level = node.get('attrs', {}).get('level', 1)
        children = node.get('children', [])
        rendered = ''.join(render_node(child) for child in children)
        return ('#' * level + ' ' + rendered + '\n')
    elif t == 'block_html':
        return node.get('raw', '')
    else:
        if 'children' in node:
            return ''.join(render_node(child) for child in node['children'])
        else:
            return node.get('raw', '')

def node_token_count(node):
    """
    Returns the token count for a given node by rendering it to markdown first.
    """
    return token_count(render_node(node))

def is_heading(node):
    """
    Determines if a node is a heading.
    In our AST, we assume a node with type 'heading' is a semantic boundary.
    """
    return node.get('type') == 'heading'

def group_by_semantic_boundaries(ast_nodes):
    """
    Groups AST nodes into sections using headings as boundaries.
    Each time a heading node is encountered, a new section starts.
    
    Parameters:
      ast_nodes (list of dict): The AST nodes in document order.
    
    Returns:
      list of lists: Each inner list is a section (a list of nodes).
    """
    sections = []
    current_section = []
    for node in ast_nodes:
        if is_heading(node):
            if current_section:
                sections.append(current_section)
            current_section = [node]
        else:
            current_section.append(node)
    if current_section:
        sections.append(current_section)
    return sections

def chunk_section(section, token_limit):
    """
    Given a section (a list of AST nodes), further break it into sub–chunks so that
    the total token count in each sub–chunk is within the token limit.
    
    If an individual node is too long (its rendered text exceeds token_limit),
    that node is split into smaller pieces using split_node().
    
    Parameters:
      section (list of dict): A section of AST nodes.
      token_limit (int): Maximum allowed token count per chunk.
    
    Returns:
      list of lists: Each inner list is a sub–chunk (list of nodes).
    """
    subchunks = []
    current_chunk = []
    current_tokens = 0

    for node in section:
        n_tokens = node_token_count(node)
        # If the node's rendered text is too long on its own, split it.
        if n_tokens > token_limit:
            rendered = render_node(node)
            split_texts = split_node(rendered, token_limit)
            for piece in split_texts:
                piece_tokens = token_count(piece)
                # If adding this piece would exceed the limit, start a new chunk.
                if current_tokens + piece_tokens > token_limit:
                    if current_chunk:
                        subchunks.append(current_chunk)
                    # Wrap the piece in a new paragraph node.
                    new_node = {'type': 'paragraph', 'children': [{'type': 'text', 'raw': piece}]}
                    current_chunk = [new_node]
                    current_tokens = piece_tokens
                else:
                    new_node = {'type': 'paragraph', 'children': [{'type': 'text', 'raw': piece}]}
                    current_chunk.append(new_node)
                    current_tokens += piece_tokens
        else:
            if current_tokens + n_tokens > token_limit:
                if current_chunk:
                    subchunks.append(current_chunk)
                current_chunk = [node]
                current_tokens = n_tokens
            else:
                current_chunk.append(node)
                current_tokens += n_tokens

    if current_chunk:
        subchunks.append(current_chunk)
    return subchunks

def chunk_ast(ast_nodes, token_limit):
    """
    Splits the entire AST into chunks that each do not exceed the token_limit.
    The algorithm first groups nodes by semantic boundaries (headings), then for
    each section either keeps it whole (if it is under the token limit) or splits it
    further using chunk_section.
    
    Parameters:
      ast_nodes (list of dict): The list of AST nodes in document order.
      token_limit (int): Maximum allowed token count per chunk.
    
    Returns:
      list of lists: Each inner list is a chunk (a list of nodes).
    """
    sections = group_by_semantic_boundaries(ast_nodes)
    chunks = []
    for section in sections:
        section_tokens = sum(node_token_count(n) for n in section)
        if section_tokens <= token_limit:
            chunks.append(section)
        else:
            subchunks = chunk_section(section, token_limit)
            chunks.extend(subchunks)
    return chunks

def construct_markdown(chunks):
    """
    Converts a list of chunks (each a list of AST nodes) into full markdown text.
    
    Parameters:
      chunks (list of lists of dict): The chunked AST nodes.
    
    Returns:
      list of str: Each string is a complete markdown chunk.
    """
    markdown_chunks = []
    for chunk in chunks:
        # Render each node in the chunk and join them together.
        md = ''.join(render_node(node) for node in chunk)
        markdown_chunks.append(md)
    return markdown_chunks

def extract_page_numbers(text):
    """
    Extracts page number markers of the form <<PAGE NUMBER: 7>> from the given text.
    
    Parameters:
        text (str): The input text containing page number markers.
        
    Returns:
        tuple: A tuple (page_numbers, cleaned_text) where:
            - page_numbers is a list of integers extracted from the markers.
            - cleaned_text is the input text with the markers removed.
    """
    # Define a regular expression pattern for the marker.
    # This pattern captures one or more digits that follow "PAGE NUMBER:".
    pattern = r'<<PAGE NUMBER:\s*(\d+)>>'
    
    # Find all occurrences (returns a list of captured groups)
    page_numbers = re.findall(pattern, text)
    # Convert extracted strings to integers
    page_numbers = [int(num) for num in page_numbers]
    
    # Remove the markers from the text.
    cleaned_text = re.sub(pattern, '', text)
    
    return page_numbers, cleaned_text

    
def create_semantic_chunks(chunks_content_dict, chunk_size):

    chunks_content_dict = dict(sorted(chunks_content_dict.items()))

    pages = chunks_content_dict

    full_text = ""

    for page_number, text in pages.items():
        text = text['content']
        full_text += text + "\n"  # Add a space to separate pages

    md = mistune.create_markdown(renderer='ast')
    ast = md(full_text)

    chunks = chunk_ast(ast, chunk_size)
    markdown_chunks = construct_markdown(chunks)

    grouped_chunks = []
    current_chunk = ''

    for i, md in enumerate(markdown_chunks):
        new_chunk = current_chunk + '\n' + md
        if token_count(new_chunk) > chunk_size:
            grouped_chunks.append(current_chunk)
            current_chunk = md
        else:
            current_chunk = new_chunk

    if len(current_chunk)>0:
        grouped_chunks.append(current_chunk)


    processed_chunks = []
    prev_pages = [1]
    for md in grouped_chunks:
        pages, cleaned_md = extract_page_numbers(md)
        if len(pages)==0:
            pages = prev_pages
        prev_pages = pages
        min_page = min(pages)
        max_page = max(pages)
        middle_page = (min_page + max_page) // 2
        source_record = chunks_content_dict[middle_page].copy()
        source_record['content'] = cleaned_md
        try:
            del source_record['embeddings']
        except Exception as e:
            pass


        source_record['firstpage'] = min_page
        source_record['lastpage'] = max_page
        source_record['pagenumber'] = middle_page

        id_str = md + str(min_page) + str(max_page) + str(middle_page)
        hash_object = hashlib.sha256()
        hash_object.update(id_str.encode('utf-8'))
        id = hash_object.hexdigest()
        source_record['id'] = id


        processed_chunks.append(source_record)
    
    return processed_chunks