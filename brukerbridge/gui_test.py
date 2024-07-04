from tkinter import Tk  # from tkinter import Tk for Python 3.x
from tkinter.filedialog import askdirectory

Tk().withdraw()  # we don't want a full GUI, so keep the root window from appearing
filename = askdirectory(
    initialdir="G:/ftp_imports"
)  # show an "Open" dialog box and return the path to the selected file
print(filename)
