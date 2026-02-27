import subprocess
from pathlib import Path
import re
import zipfile

#Definition of default paths
DATASET_ID = "davidcariboo/player-scores"
RAW_FOLDER = "data/raw"
VERSION_FILE = Path("data/raw/.dataset_version")
ZIP_PATH = Path("data/raw/player-scores.zip")

#Only Relevant csv files of the dataset
FILES_TO_EXTRACT = {
    "players.csv",
    "clubs.csv",
    "player_valuations.csv"
}

def get_remote_version():

    #Search on kaggle to extract lastUpdated field
    result = subprocess.run(
        ["kaggle", "datasets", "list", "-s", DATASET_ID],
        capture_output=True,
        text=True,
    )
    #If the search is not concluded with success raise the RunTimeError
    if result.returncode != 0:
        raise RuntimeError(f"Error searching on Kaggle:\n{result.stderr}")
    
    lines = result.stdout.splitlines()

    # Regex to capture timestamp in the following format:
    # YYYY-MM-DD HH:MM:SS(.microseconds opcional)
    timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?"

    #return de timestamp (version)
    for line in lines:
        if DATASET_ID in line:
            match = re.search(timestamp_pattern,line)
            if match:
                return match.group(0)
    
    raise ValueError("Datase doesn't contain or lastUpdated not defined")


def get_local_version():

    #check local version
    if VERSION_FILE.exists():
        return VERSION_FILE.read_text().strip()
    else:
        return None

def save_local_version(ver):

    #write new version into local version file
    VERSION_FILE.write_text(ver)

def donwload_dataset():
    print(" Downloading dataset ...")

    #download of the dataset
    subprocess.run(
        ["kaggle", "datasets","download",DATASET_ID,"-p",RAW_FOLDER,"--force",],
        check=True,
    )

    #if the donwload is not concluded with success raise the FileNotFoundError
    if not ZIP_PATH.exists():
        raise FileNotFoundError("Zip not found after donwload")
    
    print ("Extracting files..")
    
    #extract only the relevant csv
    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        for member in zip_ref.namelist():
            if member in FILES_TO_EXTRACT:
                zip_ref.extract(member, RAW_FOLDER)
    
    #delete zip folder
    ZIP_PATH.unlink()

def main():
    
    remote_ver = get_remote_version()
    local_ver = get_local_version()

    print(f"Remote Version: {remote_ver}")
    print(f"Local Version: {local_ver}")

    if (remote_ver != local_ver):
        print(" New version detected - updating raw datasets")
        donwload_dataset()
        save_local_version(remote_ver)
        print("Dataset updated!")
    else:
        print (f"Your dataset is up to date!")

if __name__ == "__main__":
    main()