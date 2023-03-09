import venv
import sys
from pathlib import Path

if __name__ == "__main__":
    rootDir = Path(__file__).parent
    envPath = rootDir / "env"
    srcPath = rootDir / "src"
    reqsPath = rootDir / "reqs.txt"

    pythonVersion = f"{sys.version_info.major}.{sys.version_info.minor}"

    sitePackagesPaths = [
        envPath / "Lib" / "site-packages",
        envPath / "lib" / f"python{pythonVersion}" / "site-packages"
    ]

    if not envPath.exists():
        print("Creating virual environment")
        envPath.mkdir(parents=True, exist_ok=True)
        venv.create(envPath)

    for path in sitePackagesPaths:
        if path.exists():
            savePath = path / "srcPath.pth"
            if savePath.exists():
                break

            print("Adding src path to site-packages")
            with open(savePath, 'w') as fp:
                fp.write(str(srcPath))
            break
    else:
        print("ERROR: No site-packages path found")
        exit()
    
    print("-" * 40)
    print("Virtual environment has been set up!\n")
    print("To enter virtual environment:\n\t- Windows: env/Scipts/activate\n\t- MacOS/Linux: source env/bin/activate\n")
    print("To install required modules run:\n\tpip install -r reqs.txt\n")
    print("Call `deactivate` to leave the virtual environment")
    print("-" * 40)
