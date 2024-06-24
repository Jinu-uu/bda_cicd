import gspread
import os
import dotenv
import requests, json
import pandas as pd
import unicodedata
import warnings
import datetime


from dotenv import load_dotenv

class homework:
    
    def __init__(self, class_name : str, class_database_id : str, week : int, day : int, time : int)->None:
        '''
        클래스 변수 정의 및 스프레드시트, 노션 로드
        '''
        self.class_name = class_name
        self.class_database_id = class_database_id
        self.week = week
        self.day = day
        self.time = time

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
        self.class_sheet = doc.worksheet(class_name)

    
    def process(self):
        '''
        클래스 함수들 돌림
        '''
        hw_data = self.getHomeWorkDB()
        hw_week_data = self.hwWeekDection(hw_data)
        hw_preprocessing_data = self.hwPreProcessing(hw_week_data)
        self.updateHWSpread(hw_preprocessing_data)
        return None



    def getHomeWorkDB(self)->list:
        '''
        - 인자 : 분반명, readWeek 반환 값(self)
        - 작동 : 해당 주차 노션 과제 DB 데이터 가져오기
        - 반환 : list
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
        '''
        노션 데이터베이스에서 얻은 과제 DB 중 이번 주에 해당되는 데이터를 추출
        '''
        column_name = [x for x in list(raw_data[0]['properties'].keys()) if f"({self.week}주차)" in x][0]
        week_hw_data = []
        for info in raw_data:
            name = ''
            hw = ''
            if info['properties']['이름']['title']:
                name = unicodedata.normalize('NFC', info['properties']['이름']['title'][0]['plain_text'])   #유니코드 정규화 ㅇㅠㄴㅣㅋㅗㄷㅡ -> 유니코드
            if info['properties'][column_name]['files']:
                hw = ' '.join([unicodedata.normalize('NFC',x['name']) for x in info['properties'][column_name]['files']])   #유니코드 정규화 ㅇㅠㄴㅣㅋㅗㄷㅡ -> 유니코드
            week_hw_data.append([name, hw])
        return week_hw_data

    def hwPreProcessing(self, hw_data : list)->pd.DataFrame:
        '''
        - 인자 : hwWeekDection에서  얻은 list
        - 작동 : 가져온 데이터 전처리
        - 반환 : pd.DataFrame
        '''

        #노션 페이지에 있는 그대로 스프레드시트에 올리면 이름이 안맞거나 과제 칸이 안맞는 등의 불상사 발생
        #그래서 스프레드시트의 이름과 노션 페이지의 이름/과제를 가져와서 이름 정규화 후 스프레드시트 기준으로 머지(없는 이름은 오류처리)

        hw_df = pd.DataFrame(hw_data, columns=['name','hw'])
        hw_df['name'] = hw_df['name'].apply(lambda x : x.split('(')[0].replace(' ','').replace('_',''))
        hw_df['필수'] = hw_df['hw'].apply(lambda x : 'O' if '필수' in x else 'X')
        hw_df['복습'] = hw_df['hw'].apply(lambda x : 'O' if '복습' in x else 'X')
        hw_df = hw_df[hw_df['name'] != ''].sort_values(by='name').reset_index()
        range_list = [x.value for x in self.class_sheet.range('A4:A400')]
        merge_df = pd.merge(pd.DataFrame(range_list, columns=['name']), hw_df, on='name', how='left').fillna('오류')
        return merge_df[merge_df['name'] != '']


    def updateHWSpread(self, hw_df:pd.DataFrame)->None:
        '''
        - 인자 : hwPreProcessing에서 얻은 데이터프레임
        - 작동 : 전처리된 데이터 사용하여 스프레드시트 업데이트
        - 반환 : null
        '''
        #클로저
        def updateCells(spread_sheet_type : str)->None:
            cell_location = cellDection(spread_sheet_type, self.week)
            cell_list = self.class_sheet.range(f'{cell_location[0]}{cell_location[1]}:{cell_location[0]}{cell_location[1]+len(hw_df)}')
            for i, val in enumerate(hw_df[spread_sheet_type]):
                cell_list[i].value = val    
            self.class_sheet.update_cells(cell_list)

        updateCells("필수")
        updateCells("복습")
        return None

    
    def weekUpdate(self, json_path : str):
        '''
        json 파일 읽고 N주차에 +1
        '''
        with open(json_path, 'rt', encoding='UTF8') as json_file:
            json_data = json.load(json_file)
        
        for x in range(len(json_data['class'])):
            if json_data['class'][x]['class_name'] == self.class_name:
                json_data['class'][x]['week'] += 1
                break


        with open('data.json', 'w', encoding='UTF8') as outfile:
            json.dump(json_data, outfile)       

        return None










class attandnace:
    def __init__(self, notion_database_id, week):
        '''
        클래스 변수 정의 및 스프레드시트, 노션 로드
        '''
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


    def process(self):
        '''
        클래스 함수 돌림
        '''
        #출석 DB로드->줌로그 파일 무결성->줌로그 다운->폴더 경로 가져오기
        attandnace_data = self.getAttandanceDB()
        self.mkdirZoomLog()
        self.downloadZoomLog(attandnace_data)
        class_list = self.getWeekZoomLogFilePath()

        for file_path in class_list:
            class_name = file_path.split('/')[-1].split('.')[0]
            class_time = attandnace_obj.getClassTime('data.json', class_name)
            file_data = self.getWeekZoomLog(file_path, class_name, class_time)
            self.updateAttandanceSpread(file_data, class_name)
        return None


    def getAttandanceDB(self)->list:
        '''
        - 인자 : self
        - 작동 : 해당 주차 노션 출석 DB 가져오기
        - 반환 : list(노션 페이지 데이터)
        '''
        params = {}
        pages_and_databases = []

        while True:
            search_response = requests.post(self.notion_database_read_url, json=params, headers=self.notion_header)
            if search_response.ok is False: break
            search_response_obj = search_response.json()	
            pages_and_databases.extend(search_response_obj.get("results"))
            params["sta rt_cursor"] = search_response_obj.get("next_cursor")

        return pages_and_databases


    def downloadZoomLog(self, data_list : list)->None:
        '''
        - 인자 : getAttandanceBD에서 list, 반환 값
        - 작동 : 가져온 데이터에서 file path를 통해 csv들 다운받기
        - 반환 : null
        '''
        column_name = [x for x in list(data_list[0]['properties'].keys()) if f"{self.week}주차" == x][0]
        row_list = [x['properties']['분반명']['title'][0]['plain_text'] for x in data_list]

        dir_path = f"zoom_logs\\{column_name}"

        for x in range(len(row_list)):
            if len(data_list[x]['properties'][column_name]['files']) != 0:
                file_name = f"{dir_path}\\{row_list[x]}.csv"
                file_url = requests.get(data_list[x]['properties'][column_name]['files'][0]['file']['url'])
                open(file_name, 'wb').write(file_url.content)
        return None

    def getWeekZoomLog(self, file_path : str, class_name : str, class_time : int)->list:
        '''
        - 인자 : 파일 경로, 분반이름 및 시간
        - 작동 : 해당 주차 모든 줌 로그와서 전처리
        - 반환 : list(csv file path list)
        '''
        #과제와 같은 방법으로 스프레드시트에서 이름 가져와서 머지
        attendance = self.doc.worksheet(class_name)

        df = pd.read_csv(file_path)

        df['이름(원래 이름)'] = df['이름(원래 이름)'].apply (lambda x : x.split('(')[0].replace(' ','').replace('_',''))
        range_list = [x.value for x in attendance.range('A4:A400')]


        #참가 시간과 나간 시간 열이 있을 경우
        if '참가 시간' in ''.join(list(df.columns)) and '나간 시간' in ''.join(list(df.columns)):
            df['참가 시간'] = pd.to_datetime(df['참가 시간'])
            df['나간 시간'] = pd.to_datetime(df['나간 시간'])
            df.drop(columns=['기간(분)'])
            def timeCal(enter_time, exit_time):
                if enter_time.hour >= 21:
                    return exit_time.hour*60+exit_time.minute - enter_time.hour*60-enter_time.minute
                else:
                    return exit_time.hour*60+exit_time.minute - class_time*60
                
            df['기간(분)'] = df.apply(lambda x : timeCal(x['참가 시간'], x['나간 시간']), axis = 1)
            
            jun_time = list(df[df['사용자 이메일'] == 'official.datachef@gmail.com']['기간(분)'])[0]
            merge_df = pd.merge(pd.DataFrame(range_list, columns=['이름(원래 이름)']), df.groupby(['이름(원래 이름)'])['기간(분)'].sum().reset_index(), on='이름(원래 이름)', how='left').fillna(-1)
            
        #참가 시간과 나간 시간 열이 없을 경우
        else:
            jun_time = list(df[df['사용자 이메일'] == 'official.datachef@gmail.com']['기간(분)'])[0]
            merge_df = pd.merge(pd.DataFrame(range_list, columns=['이름(원래 이름)']), df.groupby(['이름(원래 이름)'])['기간(분)'].sum().reset_index(), on='이름(원래 이름)', how='left').fillna(-1)
            


        merge_df['출석'] = merge_df['기간(분)'].apply(lambda x : 'O' if jun_time-10 <= x else ('지각' if x != -1 else 'X'))
        results_df = merge_df[merge_df['이름(원래 이름)'] != '']
        return results_df

    def updateAttandanceSpread(self, attandance_df : pd.DataFrame, class_name : str)->None:
        '''
        - 인자 : zoomLogPreProcessing에서 얻은 list
        - 작동 : 전처리 데이터 스프레드시트에 업데이트
        - 반환 : null
        '''
        cell_location = cellDection("출석", self.week)
        attendance = self.doc.worksheet(class_name)


        cell_list = attendance.range(f"{cell_location[0]}{cell_location[1]}:{cell_location[0]}{cell_location[1]+len(attandance_df)}")
        cell_values = list(attandance_df['출석'])
        for i, val in enumerate(cell_values):  #gives us a tuple of an index and value
            cell_list[i].value = val    #use the index on cell_list and the val from cell_values

        attendance.update_cells(cell_list)
        return None


    def mkdirZoomLog(self)->None:
        '''
        줌 로그 및 주차별 디렉토리 생성
        '''
        try: os.listdir().index('zoom_logs')
        except: os.mkdir('zoom_logs')

        try: os.listdir(f'zoom_logs/').index(f'{self.week}주차')
        except: os.mkdir(f'zoom_logs/{self.week}주차')
        return None

    def getWeekZoomLogFilePath(self)->list:
        '''
        줌 디렉토리 반환
        '''
        os_list = os.listdir(f'zoom_logs/{self.week}주차')
        return [f'./zoom_logs/{self.week}주차/{x}' for x in os_list]


    def weekUpdate(self, json_path : str):
        '''
        주차 카운팅 +1
        '''
        with open(json_path, 'rt', encoding='UTF8') as json_file:
            json_data = json.load(json_file)
        
        json_data['attandance_week']+=1

        with open('data.json', 'w', encoding='UTF8') as outfile:
            json.dump(json_data, outfile)
        return None
    
    def getClassTime(self, json_path : str, class_name : str) -> int:
        #json 파일에서 분반 시작시간 반환
        with open(json_path, 'rt', encoding='UTF8') as json_file:
            json_data = json.load(json_file)
        
        for x in json_data['class']:
            if x['class_name'] == class_name: return x['time']
        return None




def checkDate()->str:
    '''
    - 인자 : 오늘 날짜
    - 작동 : 오늘 날짜 무슨 요일인지 리턴
    - 반환 : string
    '''
    pass


def readJson(file_path : str)->dict:
    '''
    file_path input 시 json파일 리딩 후 반환
    '''
    with open(file_path, 'rt', encoding='UTF8') as file:
        json_data = json.load(file)
    return json_data


def unicodeNormalize(data_list : list)->list:
    '''
    유니코드 정규화
    '''
    return [unicodedata.normalize('NFC',x) for x in data_list]


def cellDection(spread_sheet_type : str, week : int)->list:
    '''
    스프레드시트에서 출석/복습/필수에 어울리는 열 찾기
    '''
    #수식 6+4*week+weight
    #예외 AA
    #(6+4*week+weight)/(ord('Z')-ord('A'))의 몫과 나머지 사용해서 chr
    #chr((ord('A')-1) + (5+4*(13-1))//(ord('Z')+1-ord('A'))), chr(ord('A')+(5+4*(13-1))%(ord('Z')+1-ord('A')))
    alphabet_count = ord('Z')+1-ord('A')
    if spread_sheet_type == "출석": bais = 0
    elif spread_sheet_type == "필수": bais = 2
    elif spread_sheet_type == "복습": bais = 3
    else: return None
    cell_cal = 5+4*(week-1)+bais

    if cell_cal//alphabet_count == 0:
        return [chr(ord('A')+cell_cal%alphabet_count), 4]
    else:
        return [f"{chr((ord('A')-1)+cell_cal//alphabet_count)}{chr((ord('A'))+cell_cal%alphabet_count)}",4]




if __name__ == '__main__':
    warnings.filterwarnings(action='ignore')
    # json 읽고
    # json_data = readJson('data.json')



    json_data = readJson('data.json')
    for data in json_data['class']:
        #TODO
        #IF로 요일 조건문
        print(data['class_name'], data['notion_database_id'], data['week'])
        homework_obj = homework(data['class_name'], data['notion_database_id'], data['week'], data['day'], data['time'])
        homework_obj.process()
        homework_obj.weekUpdate('data.json')    

    attandnace_obj = attandnace(json_data["attandance_notion_database_id"], json_data['attandance_week'])
    attandnace_obj.process()
    attandnace_obj.weekUpdate('data.json')



    # json_data = readJson('data.json')
    
    # day = datetime.date(datetime.now()).weekday()
    # if json_data['attandance_week'] == day:
    #     attandnace_obj = attandnace(json_data["attandance_notion_database_id"], json_data['attandance_week'])
    #     attandnace_obj.process()
    #     attandnace_obj.weekUpdate('data.json')
    
    # else:
    #     for data in json_data['class']:
    #         if json_data['day'] != day: continue
    #         print(data['class_name'], data['notion_database_id'], data['week'])
    #         homework_obj = homework(data['class_name'], data['notion_database_id'], data['week'], data['day'], data['time'])
    #         homework_obj.process()
    #         homework_obj.weekUpdate('data.json')