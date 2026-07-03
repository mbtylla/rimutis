import csv
import re

PRICE_CSV = "kainoskeitimas.csv"
TARGET_XMLS = [
    "rimutiskaina24.xml",
    "rimutiskainoslt.xml"
]

product_info = {}
with open(PRICE_CSV, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        ean_code = row.get("ean_code")
        price = row.get("price")
        
        if ean_code:
            product_info[ean_code] = {
                "price": price
            }



# Regex, kuris randa <product> bloką su ean_code ir quantity
def update_product(match):
    product_block = match.group(0)
    ean_code_match = re.search(
    r"<ean_code><!\[CDATA\[(.*?)\]\]></ean_code>",
    product_block,
    re.DOTALL
)
    if ean_code_match:
        ean_code = ean_code_match.group(1).strip()
        if ean_code in product_info:
            info = product_info[ean_code]
            price_new = info["price"]

            # Atnaujinam <price>
            product_block = re.sub(
                r"(<price><!\[CDATA\[).*?(\]\]></price>)",
                lambda m: f"{m.group(1)}{price_new}{m.group(2)}",
                product_block,
                flags=re.DOTALL
)


    return product_block


for target_xml in TARGET_XMLS:
    with open(target_xml, "r", encoding="utf-8") as f:
        xml_text = f.read()

    xml_text_new = re.sub(
        r"<product>.*?</product>",
        update_product,
        xml_text,
        flags=re.DOTALL
    )

    with open(target_xml, "w", encoding="utf-8") as f:
        f.write(xml_text_new)

    print(f"[INFO] {target_xml} atnaujintas pagal kainoskeitimas.csv. CDATA kitur išliko nepakeisti.")
