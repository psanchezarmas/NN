import json
import os

from bs4 import BeautifulSoup


class HTMLScraper:
    """
    A class to scrape HTML files and extract relevant data.

    Attributes:
        directory (str): The directory containing HTML files to scrape.
        all_data (list): A list to store extracted data from HTML files.
    """

    def __init__(self, directory):
        """
        Initializes the HTMLScraper object.

        Parameters:
            directory (str): The directory containing HTML files to scrape.
        """
        self.directory = directory
        self.all_data = []

    def scrape(self):
        """
        Scrapes HTML files in the specified directory and extracts data.
        """
        for filename in os.listdir(self.directory):
            if filename.endswith('.html'):
                file_path = os.path.join(self.directory, filename)
                html_content = self._read_html(file_path)
                if html_content:
                    data = self._parse_html(html_content, filename)
                    self.all_data.append(data)

    def _read_html(self, file_path):
        """
        Reads the content of an HTML file.

        Parameters:
            file_path (str): The path to the HTML file.

        Returns:
            str: The content of the HTML file.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except IOError as e:
            print(f"Error reading file {file_path}: {e}")
            return None

    def _parse_html(self, html_content, filename):
        """
        Parses HTML content and extracts relevant data.

        Parameters:
            html_content (str): The HTML content to parse.
            filename (str): The name of the HTML file being parsed.

        Returns:
            dict: A dictionary containing the extracted data.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
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
        """
        Cleans price value and extracts numeric and currency parts.

        Parameters:
            price_value (str): The price value to clean.

        Returns:
            tuple: A tuple containing the numeric part and currency.
        """
        if price_value:
            # Remove spaces and extract numeric parts
            numeric_part = ''.join(filter(str.isdigit, price_value))
            # Create currency field
            currency = price_value.split()[-1]
            return int(numeric_part), currency
        return None, None

    def _get_element_text(self, soup, tag, class_name):
        """
        Finds an HTML element and retrieves its text content.

        Parameters:
            soup (BeautifulSoup): The BeautifulSoup object representing the HTML.
            tag (str): The HTML tag of the element to find.
            class_name (str): The class name of the element to find.

        Returns:
            str: The text content of the HTML element.
        """
        element = soup.find(tag, class_=class_name)
        return element.get_text(strip=True) if element else None

    def _extract_params(self, soup, tag, class_name):
        """
        Extracts parameters from HTML elements and creates a dictionary.

        Parameters:
            soup (BeautifulSoup): The BeautifulSoup object representing the HTML.
            tag (str): The HTML tag of the elements to find.
            class_name (str): The class name of the elements to find.

        Returns:
            dict: A dictionary containing the extracted parameters.
        """
        params = {}
        ul = soup.find(tag, class_=class_name)
        if ul:
            for li in ul.find_all('li', class_='param'):
                label = li.find('label', class_='param-label').get_text(strip=True)
                value = li.find('strong', class_='param-value').get_text(strip=True)
                params[label] = value
        return params

    def save_to_json(self, output_file):
        """
        Saves extracted data to a JSON file.

        Parameters:
            output_file (str): The path to the JSON file.
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_data, f, ensure_ascii=False, indent=4)
        print("Data saved to", output_file)

if __name__ == "__main__":
    directory = 'data'
    output_file = 'output.json'

    scraper = HTMLScraper(directory)
    scraper.scrape()
    scraper.save_to_json(output_file)
