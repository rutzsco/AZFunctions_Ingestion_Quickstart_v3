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

def split_text(text, chunk_size, overlap, tokenizer, segment_size=5):
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

    # # Create a BlobServiceClient object which will be used to create a container client
    # blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # # Create a container client
    # container_client = blob_service_client.get_container_client(container)

    # prefix = os.path.splitext(source_file)[0]

    # blob_list = container_client.list_blobs(name_starts_with=prefix)

    # chunks_dict = {}
    # for blob in blob_list:
    #     pattern = re.compile(r'_page_(\d+)\.json')
    #     match = pattern.search(blob.name)
    #     if match:
    #         page = match.group(1)
    #         chunks_dict[int(page)] = blob.name

    # chunks_content_dict = {}
    # for key in chunks_dict.keys():
    #     blob_client = container_client.get_blob_client(chunks_dict[key])
    #     file_data = (blob_client.download_blob().readall()).decode('utf-8')

    #     # Load the file data as a JSON string
    #     file_data =  json.loads(file_data)
    #     chunks_content_dict[key] = file_data

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

        source_record = chunks_content_dict[middle_page]
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
