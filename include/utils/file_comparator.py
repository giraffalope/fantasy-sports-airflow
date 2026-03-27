import hashlib

class FileComparator:
    def __init__(self, file1: str, file2: str):
        self.file1 = file1
        self.file2 = file2

    def _file_hash(self, path: str) -> str:
        hasher = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        return hasher.hexdigest()

    def are_different_hash(self) -> bool:
        """Compare files by hash."""
        return self._file_hash(self.file1) != self._file_hash(self.file2)

    def are_different_line_by_line(self) -> bool:
        """Compare files line by line."""
        with open(self.file1, 'r') as f1, open(self.file2, 'r') as f2:
            for line1, line2 in zip(f1, f2):
                if line1 != line2:
                    return True
            # Check if any file has extra lines
            return any(f1) or any(f2)

