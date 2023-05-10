from argparse import ArgumentParser

def verify(filename1: str, filename2: str) -> bool:
    """Verifica que 2 archivos tengan el mismo contenido.

    Args:
        filename1 (str): Nombre del primer archivo.
        filename2 (str): Nombre del segundo archivo.

    Returns:
        bool: Verdadero si los archivos tienen el mismo contenido y falso en el caso contrario.
    """
    with open(filename1, "rb") as file1, open(filename2, "rb") as file2:
        return all(byte1 == byte2 for byte1, byte2 in zip(file1, file2))

if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Verificador",
        description="Verifica que dos archivos sean iguales. Destinado a verificar la integridad de archivos descomprimidos"
    )

    parser.add_argument("file1", help="Primer archivo a comparar")
    parser.add_argument("file2", help="Segundo archivo a comparar")

    args = parser.parse_args()

    print("ok" if verify(args.file1, args.file2) else "nok")