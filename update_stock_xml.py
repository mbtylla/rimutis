import csv
import requests
from pathlib import Path
from lxml import etree


CSV_URL = "https://rimutis.lt/feed/576622227852123-stocks.csv"

LOCAL_CSV = "stock_level.csv"

TARGET_XMLS = [
    "rimutiskaina24.xml",
    "rimutiskainoslt.xml",
]


def download_csv():
    print("Downloading CSV...")

    response = requests.get(
        CSV_URL,
        timeout=60
    )

    response.raise_for_status()

    Path(LOCAL_CSV).write_bytes(response.content)

    print("CSV saved:", LOCAL_CSV)



def load_stock_data():

    stock_map = {}

    with open(
        LOCAL_CSV,
        newline="",
        encoding="utf-8"
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:

            ean = row["EAN"].strip()

            if ean:
                stock_map[ean] = row["STOCK"].strip()


    print(
        "Loaded EAN:",
        len(stock_map)
    )

    return stock_map



def update_xml(xml_file, stock_map):

    print("Updating:", xml_file)


    parser = etree.XMLParser(
        strip_cdata=False
    )


    tree = etree.parse(
        xml_file,
        parser
    )

    root = tree.getroot()

    updated = 0


    for product in root.findall("product"):

        ean_node = product.find("ean_code")
        stock_node = product.find("stock")


        if ean_node is None or stock_node is None:
            continue


        ean = (
            ean_node.text.strip()
            if ean_node.text
            else ""
        )


        if ean in stock_map:

            new_stock = stock_map[ean]


            old_stock = stock_node.text


            if old_stock != new_stock:

                stock_node.text = etree.CDATA(
                    new_stock
                )

                updated += 1



    tree.write(
        xml_file,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=True
    )


    print(
        xml_file,
        "updated:",
        updated
    )



def main():

    download_csv()

    stock_map = load_stock_data()


    for xml in TARGET_XMLS:

        update_xml(
            xml,
            stock_map
        )


if __name__ == "__main__":
    main()
