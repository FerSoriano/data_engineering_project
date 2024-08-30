import requests
from bs4 import BeautifulSoup
import pandas as pd


class Countries():
    def __init__(self) -> None:
        pass
    
    def get_response(self):
        URL = 'https://www.iban.com/country-codes'
        self.response = requests.get(URL)
        return self.response

    def create_df(self) -> pd.DataFrame :
        if self.response.status_code == 200:
            soup = BeautifulSoup(self.response.content, 'html.parser')
            table = soup.find('table', id='myTable')
            
            headers = []
            rows = []

            for header in table.find_all('th'):
                headers.append(header.text.strip())

            for t_row in table.find_all('tr'):
                cells = t_row.find_all('td')
                row = [cell.text.strip() for cell in cells]
                if row:
                    rows.append(row)

            df = pd.DataFrame(rows, columns=headers)
            return df
            
        else:
            raise Exception(f"Error al acceder a la página web: {self.response.status_code}")

if __name__ == '__main__':
    data = Countries()
    data.get_response()
    data.create_df()