import csv
import errno
import json
import os
import shutil
import tempfile


class File:
    @staticmethod
    def safe_open(path, mode, encoding="utf-8"):
        """
        Open 'path' for reading or writing, creating parent directories if needed.
        """
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        return open(path, mode, encoding=encoding)

    @staticmethod
    def silent_remove(filename):
        """
        Remove a file if it exists. Ignore if it does not exist.
        """
        try:
            os.remove(filename)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    @staticmethod
    def write(filename, content, encoding="utf-8"):
        """
        Write text content to a file.
        """
        try:
            with File.safe_open(filename, "w", encoding=encoding) as f:
                f.write(content)
        except OSError as e:
            raise RuntimeError(f"Failed to write to file {filename}: {e}") from e

    @staticmethod
    def read(filename, encoding="utf-8"):
        """
        Read the entire contents of a file.
        """
        try:
            with open(filename, encoding=encoding) as f:
                return f.read()
        except OSError as e:
            raise RuntimeError(f"Failed to read file {filename}: {e}") from e

    @staticmethod
    def write_csv(filename, rows, header=None, encoding="utf-8", atomic=True):
        """
        Write rows (list of lists) to a CSV file.

        :param filename: path to output CSV
        :param rows: list of lists of data
        :param header: optional header row
        :param atomic: whether to write atomically
        """
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

        try:
            if atomic:
                dir_name = os.path.dirname(filename) or "."
                fd, temp_path = tempfile.mkstemp(dir=dir_name)
                with os.fdopen(fd, "w", encoding=encoding, newline="") as tmp_file:
                    writer = csv.writer(tmp_file, quoting=csv.QUOTE_ALL, lineterminator="\n")
                    if header:
                        writer.writerow(header)
                    writer.writerows(rows)
                shutil.move(temp_path, filename)
            else:
                with open(filename, "w", encoding=encoding, newline="") as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
                    if header:
                        writer.writerow(header)
                    writer.writerows(rows)
        except Exception as e:
            raise RuntimeError(f"Failed to write CSV file {filename}: {e}") from e

    @staticmethod
    def read_csv(filename, encoding="utf-8", as_dict=False):
        """
        Read a CSV file.

        :param filename: path to CSV
        :param as_dict: if True, return list of dicts; else list of lists
        """
        try:
            with open(filename, encoding=encoding, newline="") as f:
                if as_dict:
                    dict_reader = csv.DictReader(f)
                    return list(dict_reader)
                list_reader = csv.reader(f)
                return list(list_reader)
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV file {filename}: {e}") from e

    @staticmethod
    def write_json(filename, data, encoding="utf-8", atomic=True, indent=4):
        """
        Write a Python object as JSON to a file.

        :param filename: path to output JSON
        :param data: Python dict or list
        :param atomic: whether to write atomically
        :param indent: pretty-print JSON
        """
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

        try:
            if atomic:
                dir_name = os.path.dirname(filename) or "."
                fd, temp_path = tempfile.mkstemp(dir=dir_name)
                with os.fdopen(fd, "w", encoding=encoding) as tmp_file:
                    json.dump(data, tmp_file, ensure_ascii=False, indent=indent)
                shutil.move(temp_path, filename)
            else:
                with open(filename, "w", encoding=encoding) as f:
                    json.dump(data, f, ensure_ascii=False, indent=indent)
        except Exception as e:
            raise RuntimeError(f"Failed to write JSON file {filename}: {e}") from e

    @staticmethod
    def read_json(filename, encoding="utf-8"):
        """
        Read a JSON file into a Python object.
        """
        try:
            with open(filename, encoding=encoding) as f:
                return json.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to read JSON file {filename}: {e}") from e

    @staticmethod
    def exists(path):
        """Check if a file exists."""
        exists = os.path.exists(path)
        return exists

    @staticmethod
    def size(path):
        """Return file size in bytes, or None if not found."""
        try:
            size = os.path.getsize(path)
            return size
        except OSError:
            return None
