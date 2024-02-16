import requests
import pandas as pd
import datetime


def fetch_product_data(loja):
    total_products = 1500
    per_page = 50

    url_base = rf"https://www.{loja}.com.br/api/catalog_system/pub/products/search"

    params_base = {
        "O": "OrderByTopSaleDESC",
        "_from": 0,
        "_to": 49,
    }

    all_products = []

    for i in range(0, total_products + 1, per_page):
        params = params_base.copy()
        params["_from"] = i
        params["_to"] = i + per_page - 1
        print(f"Fetching products from {i} to {i + per_page - 1} ...")
        response = requests.get(url_base, params=params)
        print(response.url)
        products_page = response.json()
        all_products.extend(products_page)
        # printando o link do get

    return all_products


def extract_desired_information(products):
    extracted_data = []

    for item in products:
        try:
            product_id = item.get("productId", None)
            brand = item.get("brand", None)
            price = (
                item.get("items", [{}])[0]
                .get("sellers", [{}])[0]
                .get("commertialOffer", {})
                .get("Price", None)
            )
            ean = item.get("items", [{}])[0].get("ean", None)
            available_quantity = (
                item.get("items", [{}])[0]
                .get("sellers", [{}])[0]
                .get("commertialOffer", {})
                .get("AvailableQuantity", None)
            )

            extracted_data.append(
                {
                    "id": product_id,
                    "brand": brand,
                    "price": price,
                    "ean": ean,
                    "AvailableQuantity": available_quantity,
                }
            )
        except Exception as e:
            continue

    return pd.DataFrame(extracted_data)


lojas = ["balaroti", "obramax", "cassol", "nichele"]


def obtem_top_1500(loja):
    dia_mes = datetime.datetime.now().strftime("%d_%m")
    print(f"Fetching data from {loja} ...")
    products = fetch_product_data(loja)
    print(f"Extracting desired information from {loja} ...")
    df = extract_desired_information(products)
    print(f"Data from {loja} saved to {loja}.csv")
    df.to_csv(
        rf"Z:\Vitor\dados_concorrentes\top_1500\{loja}\top_1500_dia_{dia_mes}.csv",
        sep=";",
        encoding="utf-8-sig",
    )

if __name__ == "__main__":
    for loja in lojas:
        obtem_top_1500(loja)
