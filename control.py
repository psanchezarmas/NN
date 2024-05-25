import json
import os

from bs4 import BeautifulSoup4
from pyspark import SparkConf, SparkContext


class HTMLScraper:
    def __init__(self, directory):
        self.directory = directory

    def scrape(self):
        # Initialize Spark context
        conf = SparkConf().setAppName("HTMLScraper").setMaster("local[*]")
        sc = SparkContext(conf=conf)

        # Get list of files in the directory
        file_paths = [os.path.join(self.directory, filename) for filename in os.listdir(self.directory) if filename.endswith('.html')]

        # Parallelize the file paths
        file_paths_rdd = sc.parallelize(file_paths)

        # Process each file in parallel
        data_rdd = file_paths_rdd.map(self._process_file)

        # Collect the results
        self.all_data = data_rdd.collect()

        # Stop the Spark context
        sc.stop()

    def _process_file(self, file_path):
        html_content = self._read_html(file_path)
        if html_content:
            return self._parse_html(html_content, os.path.basename(file_path))
        return None

    def _read_html(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except IOError as e:
            print(f"Error reading file {file_path}: {e}")
            return None

    def _parse_html(self, html_content, filename):
        soup = BeautifulSoup4(html_content, 'html.parser')
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

    def clean_price_value(self, price_value):
        if price_value:
            # Remove spaces and extract numeric parts
            numeric_part = ''.join(filter(str.isdigit, price_value))
            # Create currency field
            currency = price_value.split()[-1]
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
                label = li.find('label', class_='param-label').get_text(strip=True)
                value = li.find('strong', class_='param-value').get_text(strip=True)
                params[label] = value
        return params

    def save_to_json(self, output_file):
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_data, f, ensure_ascii=False, indent=4)
        print("Datos guardados en", output_file)

if __name__ == "__main__":
    directory = 'data'
    output_file = 'output.json'

    scraper = HTMLScraper(directory)
    scraper.scrape()
    scraper.save_to_json(output_file)
