import gspread
import os
import dotenv
import requests, json
import pandas as pd
import unicodedata

from dotenv import load_dotenv

class homework:
    
    def __init__(self, class_name, class_database_id, week):
        self.class_name = class_name
        self.class_database_id = class_database_id
        self.week = week

        load_dotenv()
        self.notion_token = os.environ.get('NOTION_API')
        self.notion_header = {
            "Authorization":"Bearer "+ self.notion_token,
            "Notion-Version":"2022-06-28"
        }
        self.notion_database_read_url = f"https://api.notion.com/v1/databases/{self.class_database_id}/query"

        json_file_path = os.environ.get('GCP_JSON_PATH')

        gc = gspread.service_account(json_file_path)
        spreadsheet_url = os.environ.get('SHEET_URL')
        doc = gc.open_by_url(spreadsheet_url)
        self.python_attendance = doc.worksheet(class_name)

    
    def process(self):
        hw_data = self.getHomeWorkDB()
        hw_week_data = self.hwWeekDection(hw_data)
        hw_preprocessing_data = self.hwPreProcessing(hw_week_data)
        self.updateHWSpread(hw_preprocessing_data)




    def getHomeWorkDB(self)->list:
        '''
        - 인자 : 분반명, readWeek 반환 값
        - 작동 : 해당 주차 노션 과제 DB 데이터 가져오기(for문으로 함수 밖에서 분반 별로 돌리기)
        - 반환 : dict 혹은 list(결정 못함)
        '''

        params = {}
        pages_and_databases = []

        while True:
            search_response = requests.post(self.notion_database_read_url, json=params, headers=self.notion_header)
            if search_response.ok is False: break
            search_response_obj = search_response.json()	
            pages_and_databases.extend(search_response_obj.get("results"))
            params["start_cursor"] = search_response_obj.get("next_cursor")
        return pages_and_databases

    def hwWeekDection(self, raw_data : list)->list:
        column_name = [x for x in list(raw_data[0]['properties'].keys()) if self.week in x][0]
        week_hw_data = []
        for info in raw_data:
            name = ''
            hw = ''
            if info['properties']['이름']['title']:
                name = unicodedata.normalize('NFC', info['properties']['이름']['title'][0]['plain_text'])
            if info['properties'][column_name]['files']:
                hw = ' '.join([unicodedata.normalize('NFC',x['name']) for x in info['properties'][column_name]['files']])
            week_hw_data.append([name, hw])
        return week_hw_data

    def hwPreProcessing(self, hw_data : list)->pd.DataFrame:
        '''
        - 인자 : getHomeWorkDB에서 얻은 dict 혹은 list
        - 작동 : 가져온 데이터 전처리
        - 반환 : pd.DataFrame
        '''
        hw_df = pd.DataFrame(hw_data, columns=['name','hw'])
        hw_df['필수'] = hw_df['hw'].apply(lambda x : 'O' if '필수' in x else 'X')
        hw_df['복습'] = hw_df['hw'].apply(lambda x : 'O' if '복습' in x else 'X')
        hw_df = hw_df[hw_df['name'] != ''].sort_values(by='name').reset_index()

        return hw_df


    def updateHWSpread(self, hw_df:pd.DataFrame)->None:
        '''
        - 인자 : hwPreProcessing에서 얻은 dict 혹은 list
        - 작동 : 전처리된 데이터 사용하여 스프레드시트 업데이트
        - 반환 : null
        '''


        cell_list = self.python_attendance.range(f'H4:H{4+len(hw_df)}')

        for i, val in enumerate(hw_df['필수']):
            cell_list[i].value = val

        self.python_attendance.update_cells(cell_list)



        cell_list = self.python_attendance.range(f'I4:I{4+len(hw_df)}')

        for i, val in enumerate(hw_df['복습']):
            cell_list[i].value = val

        self.python_attendance.update_cells(cell_list)









class attandnace:
    def __init__(self, notion_database_id, week):
        self.notion_database_id = notion_database_id
        self.week = week

        load_dotenv()
        self.notion_token = os.environ.get('NOTION_API')
        self.notion_header = {
            "Authorization":"Bearer "+ self.notion_token,
            "Notion-Version":"2022-06-28"
        }
        self.notion_database_read_url = f"https://api.notion.com/v1/databases/{self.notion_database_id}/query"

        json_file_path = os.environ.get('GCP_JSON_PATH')

        gc = gspread.service_account(json_file_path)
        spreadsheet_url = os.environ.get('SHEET_URL')
        self.doc = gc.open_by_url(spreadsheet_url)
        pass


    def process(self):
        attandnace_data = self.getAttandanceDB()
        self.downloadZoomLog(attandnace_data)
        class_list = self.getWeekZoomLogFilePath()
        for file in class_list:
            file_data = self.getWeekZoomLog(file)
            self.updateAttandanceSpread(file_data)


    def getAttandanceDB(self)->list:
        '''
        - 인자 : readWeek 반환 값
        - 작동 : 해당 주차 노션 출석 DB 가져오기(for문X, 이 함수에서 모든 분반 처리)
        - 반환 : dict 혹은 list(결정 못함)(n주차 csv file path)
        '''
        params = {}
        pages_and_databases = []

        while True:
            search_response = requests.post(self.notion_database_read_url, json=params, headers=self.notion_header)
            if search_response.ok is False: break
            search_response_obj = search_response.json()	
            pages_and_databases.extend(search_response_obj.get("results"))
            params["start_cursor"] = search_response_obj.get("next_cursor")

        return pages_and_databases


    def downloadZoomLog(self, data_list : list)->None:
        '''
        - 인자 : getAttandanceBD에서 얻은 dict 혹은 list, readWeek 반환 값, .env.get(’zoomlog_dir’)
        - 작동 : 가져온 데이터에서 file path를 통해 csv들 다운받기
        - 반환 : null
        '''
        column_name = [x for x in list(data_list[0]['properties'].keys()) if self.week == x][0]
        row_list = [x['properties']['분반명']['title'][0]['plain_text'] for x in data_list]

        dir_path = f"zoom_logs\\{column_name}"

        for x in range(len(row_list)):
            if len(data_list[x]['properties'][column_name]['files']) != 0:
                file_name = f"{dir_path}\\{row_list[x]}.csv"
                file_url = requests.get(data_list[x]['properties'][column_name]['files'][0]['file']['url'])
                open(file_name, 'wb').write(file_url.content)

    def getWeekZoomLog(self, file_path : str)->list:
        '''
        - 인자 : readWeek 반환 값
        - 작동 : 해당 주차 모든 줌 로그 가져오기
        - 반환 : list(csv file path list)
        '''
        python_attendance = self.doc.worksheet("Python 문법 기초반")

        df = pd.read_csv(file_path)
        jun_time = list(df[df['사용자 이메일'] == 'official.datachef@gmail.com']['기간(분)'])[0]

        df['이름(원래 이름)'] = df['이름(원래 이름)'].apply(lambda x : x.split('(')[0].replace(' ','').replace('_',''))
        range_list = [x.value for x in python_attendance.range('A4:A400')]

        merge_df = pd.merge(pd.DataFrame(range_list, columns=['이름(원래 이름)']), df.groupby(['이름(원래 이름)'])['기간(분)'].sum().reset_index(), on='이름(원래 이름)', how='left').fillna(0)
        
        merge_df['출첵'] = merge_df['기간(분)'].apply(lambda x : 'O' if jun_time-10 <= x else ('지각' if x != 0 else 'X'))
        results_df = merge_df[merge_df['이름(원래 이름)'] != '']
        return results_df

    def updateAttandanceSpread(self, attandance_df : pd.DataFrame)->list:
        '''
        - 인자 : zoomLogPreProcessing에서 얻은 dict 혹은 list
        - 작동 : 전처리 데이터 스프레드시트에 업데이트
        - 반환 : null
        '''

        python_attendance = self.doc.worksheet("Python 문법 기초반")

        cell_list = python_attendance.range(f"F{4}:F{4+len(attandance_df)}")
        cell_values = list(attandance_df['출첵'])
        for i, val in enumerate(cell_values):  #gives us a tuple of an index and value
            cell_list[i].value = val    #use the index on cell_list and the val from cell_values

        python_attendance.update_cells(cell_list)

    def getWeekZoomLogFilePath(self)->list:
        os_list = os.listdir(f'zoom_logs/{self.week}')
        return [f'./zoom_logs/{self.week}/{x}' for x in os_list]


def checkDate()->str:
    '''
    - 인자 : 오늘 날짜
    - 작동 : 오늘 날짜 무슨 요일인지 리턴
    - 반환 : string
    '''
    pass

def readWeek()->int:
    '''
    - 인자 : json file path
    - 작동 : 오늘 몇주차인지 리턴
    - 반환 : int
    '''
    pass

def getWeekZoomLogFilePath(week : str)->list:
    os_list = os.listdir(f'zoom_logs/{week}')
    return [f'./zoom_logs/{week}/{x}' for x in os_list]


def unicodeNormalize(data_list : list)->list:
    return [unicodedata.normalize('NFC',x) for x in data_list]


if __name__ == '__main__':
    # json 읽고
    # for x in json:
        # homework(분반, week).start()

    # homework("Python 문법 기초반","d84cffb8266b4c5bb7bbc0252c6c817d","(6주차)").process()
    attandnace("3a2a9742f1fa40f18c432a06401310cf", "6주차").process()
