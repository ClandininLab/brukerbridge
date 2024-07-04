import os


def main(directory, ripper_done_file):
    print("Checking for raw files in directory {}".format(directory))
    for item in os.listdir(directory):
        new_path = directory + "/" + item

        # Check if item is a directory
        if os.path.isdir(new_path):
            check_for_raw_file(new_path, ripper_done_file)

        # If the item is a file
        else:
            if "_RAWDATA_" in item:
                with open(ripper_done_file, "w") as file:
                    file.write("False")
                print("Found raw file {}".format(item))
                break


if __name__ == "__main__":
    main()
