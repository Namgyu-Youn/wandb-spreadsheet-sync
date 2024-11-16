import math
import os
from datetime import datetime
import schedule
import json
import time
import argparse
from tqdm import tqdm
import logging
from typing import Tuple, List, Dict, Any

import wandb
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('wandb_sync.log')
    ]
)
logger = logging.getLogger(__name__)

class ConfigError(Exception):
    """Configuration related errors"""
    pass

class SheetError(Exception):
    """Google Sheets related errors"""
    pass

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Sync WandB runs to Google Sheets')
    parser.add_argument('--schedule-time', type=int, default=30,
                       help='Schedule interval in minutes (default: 30)')
    parser.add_argument('--user-name', type=str, default='Anoymous',
                       help='User name for tracking WandB runs')
    parser.add_argument('--sheet-name', type=str, required=True,
                       help='Name of the Google Sheet to use')
    parser.add_argument('--config-path', type=str, default='config.json',
                       help='Path to configuration file')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of rows to process at once')
    return parser.parse_args()

def get_wandb_project_info() -> Tuple[str, str]:
    """현재 실행 중인 WandB 프로젝트 정보 가져오기"""
    current_run = wandb.run
    if current_run is None:
        raise ConfigError("No active WandB run found")

    project_name = current_run.project
    entity_name = current_run.entity  # entity는 team name과 동일

    if not project_name or not entity_name:
        raise ConfigError("Failed to get project or team name from WandB run")

    return entity_name, project_name

def load_config(config_path: str) -> Dict[str, Any]:
    """설정 파일 로드 및 검증"""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        required_keys = ['GCP_JSON', 'FIXED_HEADERS']  # TEAM_NAME과 PROJECT_NAME은 더 이상 필요하지 않음
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ConfigError(f"Missing required keys in config: {missing_keys}")

        # WandB에서 현재 프로젝트 정보 가져오기
        try:
            team_name, project_name = get_wandb_project_info()
            config['TEAM_NAME'] = team_name
            config['PROJECT_NAME'] = project_name
            logger.info(f"Using WandB project: {project_name} from team: {team_name}")
        except ConfigError as e:
            raise ConfigError(f"Failed to get WandB project info: {str(e)}")

        return config
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {config_path}")
    except json.JSONDecodeError:
        raise ConfigError(f"Invalid JSON in config file: {config_path}")

def init_sheet(sheet_name: str, config: Dict[str, Any]) -> Tuple[gspread.Worksheet, wandb.Api]:
    """스프레드시트 초기화 및 WandB API 연결"""
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            config['GCP_JSON'], scope
        )
        client = gspread.authorize(creds)

        # Open spreadsheet
        spreadsheet = client.open(sheet_name)

        # 시트 개수 제한 확인
        if len(spreadsheet.worksheets()) >= 100:  # Google Sheets 제한
            oldest_sheet = min(spreadsheet.worksheets(),
                             key=lambda x: x.title if not x.title.startswith('runs_')
                             else x.title.split('_')[1])
            oldest_sheet.delete()
            logger.warning(f"Deleted oldest sheet: {oldest_sheet.title}")

        # If there is no existing sheet, create it.
        if len(spreadsheet.sheet1.get_all_values()) > 0:
            new_sheet_name = f"runs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            worksheet = spreadsheet.add_worksheet(
                title=new_sheet_name,
                rows=min(1000, spreadsheet.sheet1.row_count),
                cols=min(50, spreadsheet.sheet1.col_count)
            )
            # Copy before sheet.
            header_row = spreadsheet.sheet1.row_values(1)
            if header_row:
                worksheet.append_row(header_row)
        else:
            worksheet = spreadsheet.sheet1

        # WandB API 연결
        api = wandb.Api()

        return worksheet, api

    except Exception as e:
        raise SheetError(f"Failed to initialize sheet: {str(e)}")

def process_runs_batch(runs: List[Any], run_id_list: List[str],
                      final_headers: List[str], user_name: str,
                      batch_size: int) -> List[List[str]]:
    """배치 단위로 WandB runs 처리"""
    rows_to_add = []
    batch_runs = []

    for run in runs:
        if run.state == "finished" and run.id not in run_id_list:
            if run.user.name == user_name:
                batch_runs.append(run)

                if len(batch_runs) >= batch_size:
                    rows_to_add.extend(process_batch(batch_runs, final_headers))
                    batch_runs = []

    if batch_runs:  # 남은 runs 처리
        rows_to_add.extend(process_batch(batch_runs, final_headers))

    return rows_to_add

def process_batch(batch_runs: List[Any], final_headers: List[str]) -> List[List[str]]:
    """단일 배치 처리"""
    batch_data = []
    for run in tqdm(batch_runs, desc="Processing batch"):
        try:
            row_data = [
                run.id,
                get_timestamp(run),
                run.user.name,
            ]
            # 추가 필드 처리
            for key in final_headers[3:]:
                value = get_run_value(run, key)
                row_data.append(value)
            batch_data.append(row_data)
        except Exception as e:
            logger.error(f"Error processing run {run.id}: {str(e)}")
            continue
    return batch_data

def get_timestamp(run: Any) -> str:
    """타임스탬프 추출"""
    try:
        return (datetime.fromtimestamp(run.summary["_timestamp"])
                .strftime("%Y-%m-%d %H:%M:%S")
                if "_timestamp" in run.summary else "")
    except Exception:
        return ""

def get_run_value(run: Any, key: str) -> str:
    """run에서 값 추출"""
    try:
        if key in run.config:
            return str(run.config[key])
        elif key in run.summary:
            return str(run.summary[key])
        return ""
    except Exception:
        return ""

def sync_data(sheet: gspread.Worksheet, new_rows: List[List[str]],
              existing_data: List[List[str]]) -> None:
    """Data sync"""
    try:
        merged_data = new_rows + existing_data
        merged_data.sort(key=lambda x: x[1], reverse=True)

        # Limit computation amount using batch_size
        for i in range(0, len(merged_data), 100):  # Google Sheets API 제한
            batch = merged_data[i:i + 100]
            sheet.append_rows(batch)
            time.sleep(1)  # API 제한 방지

    except Exception as e:
        raise SheetError(f"Failed to sync data: {str(e)}")

def main(args: argparse.Namespace) -> None:
    try:
        config = load_config(args.config_path)
        sheet, api = init_sheet(args.sheet_name, config)

        runs = api.runs(f"{config['TEAM_NAME']}/{config['PROJECT_NAME']}")
        run_id_list = [row[0] for row in sheet.get_all_values()[1:]]  # Skip header

        new_rows = process_runs_batch(
            runs, run_id_list, config['FIXED_HEADERS'],
            args.user_name, args.batch_size
        )

        if new_rows:
            sync_data(sheet, new_rows, [])
            logger.info(f"Successfully added {len(new_rows)} new runs")
        else:
            logger.info("No new runs to add")

    except Exception as e:
        logger.error(f"Error in main sync process: {str(e)}")
        raise

if __name__ == "__main__":
    args = parse_args()
    logger.info(f"Starting sync process (Schedule: every {args.schedule_time} minutes)")
    logger.info(f"Monitoring runs for user: {args.user_name}")

    schedule.every(args.schedule_time).minutes.do(lambda: main(args))

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Sync process stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            time.sleep(60)  # Retry 1 min later if error occurs.
