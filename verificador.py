from argparse import ArgumentParser

def verify(filename1: str, filename2: str) -> bool:
    with open(filename1, "rb") as file1, open(filename2, "rb") as file2:
        return all(byte1 == byte2 for byte1, byte2 in zip(file1, file2))
    
if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Verificador",
        description="Verifica la integridad de un archivo descomprimido"
    )

    parser.add_argument("filename", help="Archivo txt a comparar")
    parser.add_argument("-d", "--decompressed", help="Archivo descomprimido a comparar", default="descomprimido-elmejorprofesor.txt")

    args = parser.parse_args()

    print("ok" if verify(args.filename, args.decompressed) else "nok")