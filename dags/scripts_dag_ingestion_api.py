import os
import glob
import pandas as pd
from datetime import date
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def processing_user(ti, compression: str, path: str, filename: str) -> None:     
    """
    Processes user data received from the task 'get_users', converts it to a Pandas DataFrame, 
    saves the DataFrame in the parquet format to the specified local path and raises an exception if the 
    user data is empty. 

    Args:
        ti (TaskInstance): The TaskInstance object from which to pull the output of the 'get_users' task.
        compression (str): The compression algorithm to use while saving the parquet file. Can be one of 
            'snappy', 'gzip', 'brotli' or 'None'.
        path (str): The local path where the parquet file will be saved.
        filename (str): The filename of the parquet file.

    Returns: None. The function does not return anything.
    """

    users = ti.xcom_pull(task_ids=['get_users'])
    pd.set_option('display.max_columns', None)
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    
    df = pd.json_normalize(users[0]['results'])
    df2 = pd.DataFrame(df, dtype=str)    

    try:
        df2.to_parquet(path=f'{path}' + str(date.today()) +  f'_{filename}', compression=compression, index=False)
        print('\n' + '*'*99 + '\nFile Saved Successfully in Local Folder!\n'+ '*'*99)
    except Exception as e:        
        print('\n' + '*'*99 + f'\n{e}\n'+ '*'*99)

def local_to_s3(bucket_name: str, dir_target: str, layer: str, filepath: str) -> None: 
    """
    Uploads local file to S3.

    Args:
        bucket_name (str): The name of the bucket.
        dir_target (str): The target directory in the S3 bucket.
        ds (str): The execution date in YYYY-MM-DD format.
        filepath (str): The file path of the file to be uploaded to S3.

    Returns: None. The function does not return anything.

    Raises:
        ValueError: If the directory is empty and there are no files to copy.
        Exception: If there are any issues during the file upload process.
    """

    s3 = S3Hook()
    if glob.glob(filepath):
        try:
            for f in glob.glob(filepath):
                print(f'File to move {f}.')
                key = dir_target+layer+'/'+f.split('/')[-1]
                
                print('*'*99 + f'\n{key}\n'+ '*'*99)
                
                s3.load_file(
                    filename=f,
                    bucket_name=bucket_name,
                    replace=True,
                    key=key
                    )
        except Exception as e:
            print('\n' + '*'*99 + f'\n{e}\n'+ '*'*99)
    else:
        raise ValueError('Directory Is Empty No File To Copy')
    
def remove_local_files(filepath: str) -> None:
    """
    Removes the local file(s) specified by the filepath.
    
    Args: filepath (str): Path to the file(s) to be removed.
    
    Raises: FileNotFoundError: If no files were found at the specified filepath.
    """
    files=glob.glob(filepath)
    if not files:
        raise FileNotFoundError(f'Could not find any files at {filepath}.')
    for f in files:
        os.remove(f)