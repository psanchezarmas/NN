import os

from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, MapType, StringType, StructField,
                               StructType)


class HTMLScraper:
    def __init__(self, directory):
        self.directory = directory
        self.all_data = []

    def scrape(self):
        for filename in os.listdir(self.directory):
            if filename.endswith('.html'):
                file_path = os.path.join(self.directory, filename)
                html_content = self._read_html(file_path)
                if html_content:
                    data = self._parse_html(html_content, filename)
                    if data:  # Check if data parsing was successful
                        self.all_data.append(data)

    def _read_html(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except IOError as e:
            print(f"Error reading file {file_path}: {e}")
            return None

    def _parse_html(self, html_content, filename):
        soup = BeautifulSoup(html_content, 'html.parser')
        try:
            price_value = self._get_element_text(soup, 'span', 'norm-price ng-binding')
            location_value = self._get_element_text(soup, 'span', 'location-text ng-binding')
            params1 = self._extract_params(soup, 'ul', 'params1')
            params2 = self._extract_params(soup, 'ul', 'params2')
            
            price_value_cleaned, currency = self.clean_price_value(price_value)

            return {
                'filename': filename,
                'price_value': price_value_cleaned,
                'currency': currency,
                'location_value': location_value,
                'params1': params1,
                'params2': params2
            }
        except Exception as e:
            print(f"Error parsing file {filename}: {e}")
            return None

    def clean_price_value(self, price_value):
        if price_value:
            # Remove spaces and extract numeric parts
            numeric_part = ''.join(filter(str.isdigit, price_value))
            # Create currency field
            currency = ''.join(filter(lambda x: not x.isdigit(), price_value)).strip()
            return int(numeric_part), currency
        return None, None

    def _get_element_text(self, soup, tag, class_name):
        element = soup.find(tag, class_=class_name)
        return element.get_text(strip=True) if element else None

    def _extract_params(self, soup, tag, class_name):
        params = {}
        ul = soup.find(tag, class_=class_name)
        if ul:
            for li in ul.find_all('li', class_='param'):
                label = li.find('label', class_='param-label')
                value = li.find('strong', class_='param-value')
                if label and value:
                    params[label.get_text(strip=True)] = value.get_text(strip=True)
        return params

    def display_table(self):
        try:
            spark = SparkSession.builder.master("local[1]") \
                .appName("HTML Scraper") \
                .getOrCreate()

            schema = StructType([
                StructField("filename", StringType(), True),
                StructField("price_value", IntegerType(), True),
                StructField("currency", StringType(), True),
                StructField("location_value", StringType(), True),
                StructField("params1", MapType(StringType(), StringType()), True),
                StructField("params2", MapType(StringType(), StringType()), True)
            ])

            df = spark.createDataFrame(self.all_data, schema)
            df.show()
            
            spark.stop()
        except Exception as e:
            print(f"Error displaying table: {e}")


if __name__ == "__main__":
    directory = 'data'
  

    scraper = HTMLScraper(directory)
    scraper.scrape()
    scraper.display_table()
