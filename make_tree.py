import os
import argparse
from pathlib import Path

def write_tree(root: str, out_file: str, ignore=None, max_depth=None, encoding="utf-8"):
    root_path = Path(root).resolve()
    ignore = set(ignore or [])

    lines = [root_path.name]

    def _dir_entries(p: Path):
        try:
            items = [e for e in p.iterdir() if e.name not in ignore]
        except PermissionError:
            return [], []
        dirs = sorted([e for e in items if e.is_dir()])
        files = sorted([e for e in items if e.is_file()])
        return dirs, files

    def _walk(p: Path, prefix: str, depth: int):
        if max_depth is not None and depth > max_depth:
            return
        dirs, files = _dir_entries(p)
        entries = dirs + files
        for i, e in enumerate(entries):
            connector = "└── " if i == len(entries) - 1 else "├── "
            lines.append(prefix + connector + e.name)
            if e.is_dir():
                extension = "    " if i == len(entries) - 1 else "│   "
                _walk(e, prefix + extension, depth + 1)

    _walk(root_path, "", 1)

    Path(out_file).parent.mkdir(parents=True, exist_ok=True)
    Path(out_file).write_text("\n".join(lines), encoding=encoding)

def parse_args():
    p = argparse.ArgumentParser(description="Сохранить дерево проекта в TXT.")
    p.add_argument("-r", "--root", default=".", help="Корень проекта (путь). По умолчанию: текущая папка.")
    p.add_argument("-o", "--out", default="project_tree.txt", help="Куда сохранить .txt")
    p.add_argument("-i", "--ignore", nargs="*", default=[".git", "node_modules", ".venv", "__pycache__"],
                   help="Список имён для пропуска")
    p.add_argument("-d", "--depth", type=int, default=None, help="Макс. глубина (None — без ограничений)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    write_tree(root=args.root, out_file=args.out, ignore=args.ignore, max_depth=args.depth)
    print(f"Готово: {args.out}")
