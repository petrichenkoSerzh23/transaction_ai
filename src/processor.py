_output_dir_cleared = False

def clear_output_dir(output_dir: Path):
    """
    Удаляет все файлы в папке с результатами
    """
    if not output_dir.exists():
        return

    for item in output_dir.iterdir():
        if item.is_file():
            item.unlink()

    print(f"The folder is cleared: {output_dir}")
    



def save_result(df, output_path: Path, description: str):
  

    global _output_dir_cleared

    output_dir = output_path.parent

    
    if not _output_dir_cleared:
        clear_output_dir(output_dir)
        _output_dir_cleared = True

    
    output_dir.mkdir(parents=True, exist_ok=True)

    
    df.to_csv(output_path, index=False)

    
    description_path = output_path.with_suffix(".txt")
    with open(description_path, "w", encoding="utf-8") as f:
        f.write(description)

    print(f"The result is saved: {output_path.name}")