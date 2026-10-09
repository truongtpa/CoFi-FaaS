from pathlib import Path


class J2FileEditor:
    """Load, replace và save file j2"""

    def __init__(self, path: str):
        self.path = Path(path)
        self.content = ""

        if self.path.exists():
            self.load()

    def load(self):
        """Load file"""
        with open(self.path) as f:
            self.content = f.read()
        return self

    def replace(self, old, new):
        """Replace string"""
        self.content = self.content.replace(str(old), str(new))
        return self

    def save(self, output_path: str = None):
        """Save file"""
        output_path = output_path or str(self.path)
        with open(output_path, 'w') as f:
            f.write(self.content)
        return output_path

    def __str__(self):
        return self.content