from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import os
from sqlalchemy.util import pickle
import wget
import sys
from pathlib import Path
from airflow.decorators import task
import chromadb
from airflow.utils.trigger_rule import TriggerRule

# Add plugins directory to Python path
AIRFLOW_HOME = Path("/opt/airflow")
sys.path.append(str(AIRFLOW_HOME))
from jobs.download import DATASETS, get_dataset_names
from jobs.utils import check_src_data
from airflow.operators.empty import EmptyOperator


def sanitize_bucket_name(name: str) -> str:
    """Convert dataset name to valid S3/MinIO bucket name."""
    return name.replace("_", "-").lower()


# Configuration - có thể set qua Airflow Variables
DATASET_NAME = os.getenv("DATASET_NAME", "environment_battery")  # Default dataset
dataset_folder = os.getenv("INLINE_DATA_VOLUME")
directory_chromadb = os.getenv("PERSIST_DIRECTORY")

# Dynamic naming with sanitized bucket name
MINIO_PATH = f"rag-pipeline-{sanitize_bucket_name(DATASET_NAME)}/chunks.pkl"
collection_name = (
    f"rag-pipeline-{DATASET_NAME}"  # ChromaDB collection có thể dùng underscore
)
dataset_subfolder = os.path.join(dataset_folder, DATASET_NAME)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 1, 1, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@task()
def start_task():
    """Downloads papers if they don't exist in the dataset folder."""
    folder_path = str(dataset_subfolder)
    os.makedirs(folder_path, exist_ok=True)

    dataset_files = DATASETS[DATASET_NAME]["data"]

    for file_link in dataset_files:
        dest_file_path = os.path.join(folder_path, f"{file_link['title']}.pdf")
        try:
            if not check_src_data(dest_file_path):
                print(f"Downloading {file_link['title']}...")
                wget.download(file_link["url"], out=dest_file_path)
            else:
                print(
                    f"File {file_link['title']}.pdf already exists, skipping download."
                )
        except Exception as e:
            print(f"\n⚠️  WARNING: Unexpected error downloading '{file_link['title']}': {e}")
    return {"status": "completed", "folder_path": folder_path, "dataset": DATASET_NAME}


@task.branch()
def check_collection_task(data):
    """
    Checks if the ChromaDB collection exists and compares files on disk with those in ChromaDB.
    """
    try:
        client = chromadb.HttpClient(host="chromadb-server", port=8000)
        
        # 1. Check if collection exists
        try:
            col = client.get_collection(name=collection_name)
        except chromadb.errors.NotFoundError:
            return "create_class"
            
        # 2. Get list of actual PDF files on disk
        files_on_disk = {
            os.path.join(dataset_subfolder, f) 
            for f in os.listdir(dataset_subfolder) 
            if f.endswith('.pdf')
        }
        
        # 3. Get list of already vectorized files in ChromaDB
        results = col.get(include=['metadatas'])
        files_in_chroma = {
            meta.get('source') 
            for meta in results['metadatas'] 
            if meta and meta.get('source')
        }
        
        # 4. Compare difference
        new_files = files_on_disk - files_in_chroma
        if new_files:
            print(f"New files detected on disk: {new_files}. Rebuilding collection to avoid duplicates...")
            client.delete_collection(name=collection_name)
            return "create_class"
            
        print("No new files detected. Skipping ETL process.")
        return "class_already_exists"
        
    except Exception as e:
        print(f"Unexpected error checking collection: {e}")
        return "create_class"


@task()
def create_class():
    """Creates a new class in ChromaDB."""
    print("Creating a new class in ChromaDB...")
    return True  # Indicate that the class was created


@task()
def class_already_exists():
    """Handles the case when the class already exists in ChromaDB."""
    print("Class already exists in ChromaDB.")
    return False  # Indicate that no class was created


@task()
def load_and_chunk_data():
    from jobs.load_and_chunk import LoadAndChunk
    import os
    import gc
    
    loader = LoadAndChunk()
    pdf_files = loader.load_dir(dataset_subfolder)
    
    uploaded_chunk_files = []
    bucket_name = f"rag-pipeline-{sanitize_bucket_name(DATASET_NAME)}"
    
    for file_path, chunks in loader.read_and_chunk_generator(pdf_files):
        base_name = os.path.basename(file_path)
        s3_path = f"{bucket_name}/chunks/{base_name}.pkl"
        
        print(f"-> Uploading {len(chunks)} chunks for {base_name} to {s3_path}...")
        loader.ingest_to_minio(chunks, s3_path)
        uploaded_chunk_files.append(s3_path)
        
        del chunks
        gc.collect()
        
    return {"status": "completed", "uploaded_chunk_files": uploaded_chunk_files}


@task(trigger_rule=TriggerRule.ONE_SUCCESS)
def embed_and_store_data(load_chunk_result: dict):
    from jobs.embed_and_store import DocumentEmbedder
    import gc
    
    uploaded_chunk_files = load_chunk_result.get("uploaded_chunk_files", [])
    print(f"Found {len(uploaded_chunk_files)} chunk files to embed.")
    
    embedder = DocumentEmbedder()
    for s3_path in uploaded_chunk_files:
        print(f"-> Downloading chunks from {s3_path}...")
        buffer = embedder.minio_loader.download_object_as_stream(s3_path)
        splits = pickle.load(buffer)
        
        print(f"-> Embedding and storing {len(splits)} chunks...")
        vectordb = embedder.document_embedding_vectorstore(
            splits, collection_name, directory_chromadb
        )
        
        del splits
        gc.collect()
        
    return {"status": "completed"}


# Create DAG
with DAG(
    "ingest_data",
    default_args=default_args,
    description="A DAG to ingest data",
    schedule=None,
) as dag:

    # Tasks
    start = start_task()
    branch = check_collection_task(start)
    create = create_class()
    exists = class_already_exists()
    load_chunk = load_and_chunk_data()
    embed_store = embed_and_store_data(load_chunk)
    end_task = EmptyOperator(
        task_id="end_task",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    # Task flow
    start >> branch
    branch >> [create, exists]  # Branching
    create >> load_chunk >> embed_store  # Process path
    exists >> end_task  # Skip path
    embed_store >> end_task
