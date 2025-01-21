import re
import json
import sys, os
import urllib3
import pandas as pd
from datetime import datetime


class JMA_Earthquake():
    def __init__(self):
        self.data_dir = f'./data'
        self.json_file_path = os.path.join(self.data_dir, 'jma_data.json')
        self.csv_file_path = os.path.join(self.data_dir, 'jma_data.csv')
        self.http = urllib3.PoolManager()
        
    def load_json(self, file_path):
        with open(file_path, "r") as f:
            data = json.load(f)
        
        return data

    def save_json(self, file_path, data):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as fp:
            json.dump(data, fp, ensure_ascii=False, indent=4)

    def safe_float(self, value, default=0.0):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def parse_coordinates(self, coord_string):
        if not isinstance(coord_string, str):
            return None, None, None  

        match = re.match(r'([+|-]\d{2}.\d+)([+|-]\d{3}.\d+)(.*)/', coord_string)

        if match:
            return tuple(map(self.safe_float, match.groups()))
        else:
            print(f"[Warning] Invalid coordinates format: {coord_string}")
            return None, None, None
    
    def convert_to_datetime(self, date):
        if date:
            try:
                format = '%Y%m%d%H%M%S'
                dt_datetime = datetime.strptime(date, format)
                return dt_datetime
            except ValueError:
                print(f"[Warning] Invalid datetime format: {date}")
                return None
        return None
    
    def convert_to_dataframe(self, dataList):
        records = []
        
        for data in dataList:
            base_info = {
                "ctt": self.convert_to_datetime(data.get("ctt")),
                "eid": self.convert_to_datetime(data.get("eid")),
                "rdt"      : data.get("rdt"),
                "anm"      : data.get("anm"),
                "en_anm"   : data.get("en_anm"),
                "mag": data.get("mag", "0.0"),
                "maxi": data.get("maxi", "0"),
                "latitude" : None,
                "longitude": None,
                "depth"    : None
            }
            
            if "cod" in data and data["cod"]:
                base_info["latitude"], base_info["longitude"], base_info["depth"] = self.parse_coordinates(data["cod"])

            if "int" in data and isinstance(data["int"], list):
                for region in data["int"]:
                    region_code = region.get("code", "")
                    region_maxi = region.get("maxi", "")

                    if "city" in region and isinstance(region["city"], list):
                        for city in region["city"]:
                            city_code = city.get("code", "")
                            city_maxi = city.get("maxi", "")
                            
                            records.append({
                                **base_info,
                                "region_code": region_code,
                                "region_maxi": region_maxi,
                                "city_code": city_code,
                                "city_maxi": city_maxi
                            })
                    else:
                        records.append({
                            **base_info,
                            "region_code": region_code,
                            "region_maxi": region_maxi,
                            "city_code": None,
                            "city_maxi": None
                        })
            else:
                records.append({
                    **base_info,
                    "region_code": None,
                    "region_maxi": None,
                    "city_code": None,
                    "city_maxi": None
                })
                
        df_data = pd.DataFrame(records)
        
        return df_data


    def load_API_data(self):
        url = f"https://www.jma.go.jp/bosai/quake/data/list.json"
        
        try:
            response = self.http.request(method='GET', url=url)
        except urllib3.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
        except urllib3.exceptions.RequestError as e:
            print(f"Request Error: {e}")
        else:
            print(f"Response Status: {response.status}")
            
        jma_data = json.loads(response.data)
        self.save_json(file_path=self.json_file_path, data=jma_data)
        
        if jma_data is not None:
            df_data = self.convert_to_dataframe(jma_data)
            
        df_data.to_csv(self.csv_file_path, index=False)
        
        return df_data


if __name__ == "__main__":
    jma = JMA_Earthquake()
    df_data = jma.load_API_data()
    