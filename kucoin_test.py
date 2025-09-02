import asyncio
import websockets
import json
import logging
import sys
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import time
import threading
from dotenv import load_dotenv
import os
import urllib3
import requests

# Suppress urllib3 retry warnings (Google API retries are normal)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

load_dotenv()

class Config:
    # KuCoin URLs
    REST_BASE_URL = 'https://api-futures.kucoin.com'
    WEBSOCKET_TOKEN_URL = f'{REST_BASE_URL}/api/v1/bullet-public'
    
    # Google Sheets
    CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'service-account-key.json')
    SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
    WORKSHEET_ID = int(os.getenv('GOOGLE_WORKSHEET_ID'))
    
    # Sheet positioning - simplified approach
    START_ROW = 70
    
    # Target instruments - organized by settlement date
    BTC_INSTRUMENTS = ['XBTUSDTM', 'XBTUSDM', 'XBTUSDCM', 'XBTMU25']
    ETH_INSTRUMENTS = ['ETHUSDTM', 'ETHUSDM', 'ETHUSDCM']
    TARGET_INSTRUMENTS = BTC_INSTRUMENTS + ETH_INSTRUMENTS
    
    # SAFE: Only update these columns (A-I), never touch J+
    DATA_COLUMNS = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
    PROTECTED_COLUMN_START = 'J'  # Everything from J onwards is protected
    
    # Update settings
    UPDATE_INTERVAL = int(os.getenv('UPDATE_INTERVAL_SECONDS', '3'))
    LOG_LEVEL = 'INFO'

class KuCoinWebSocketTracker:
    def __init__(self):
        self.websocket = None
        self.connected = False
        self.ws_token = None
        self.ws_endpoint = None
        
        # Data storage
        self.instruments_data = {}
        self.contracts_info = {}  # Store contract specifications
        self.last_update = 0
        self.lock = threading.Lock()
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, Config.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup Google Sheets
        self.setup_sheets()
        self.identify_sheet_structure()
        
        # Fetch contract information
        self.fetch_contract_info()

    def fetch_contract_info(self):
        """Fetch contract specifications for expiration dates"""
        try:
            for symbol in Config.TARGET_INSTRUMENTS:
                url = f"{Config.REST_BASE_URL}/api/v1/contracts/{symbol}"
                response = requests.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('code') == '200000' and data.get('data'):
                        contract_data = data['data']
                        self.contracts_info[symbol] = {
                            'expireDate': contract_data.get('expireDate'),
                            'multiplier': contract_data.get('multiplier', 1),
                            'type': contract_data.get('type', 'FFWCSX'),
                            'symbol': symbol
                        }
                        
                        if contract_data.get('expireDate'):
                            expiry_dt = datetime.fromtimestamp(contract_data['expireDate'] / 1000, timezone.utc)
                            self.logger.info(f"{symbol}: Expires {expiry_dt.strftime('%Y-%m-%d %H:%M UTC')}")
                        else:
                            self.logger.info(f"{symbol}: Perpetual contract (no expiry)")
                else:
                    self.logger.warning(f"Failed to get contract info for {symbol}: {response.status_code}")
                    
        except Exception as e:
            self.logger.error(f"Error fetching contract info: {e}")

    def setup_sheets(self):
        """Initialize Google Sheets connection with retry logic"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                creds = Credentials.from_service_account_file(Config.CREDENTIALS_FILE, scopes=scope)
                self.gc = gspread.authorize(creds)
                self.sheet = self.gc.open_by_key(Config.SHEET_ID)
                self.worksheet = self.sheet.get_worksheet_by_id(Config.WORKSHEET_ID)
                
                self.logger.info("Connected to Google Sheets with column-based protection (A-I only)")
                return
                
            except Exception as e:
                self.logger.warning(f"Sheets connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    self.logger.error("All Google Sheets connection attempts failed")
                    sys.exit(1)

    def identify_sheet_structure(self):
        """Simple approach: Find currency sections dynamically"""
        try:
            all_values = self.worksheet.get_all_values()
            self.currency_positions = {}
            
            for row_idx, row in enumerate(all_values):
                if row and len(row) > 0:
                    cell_value = str(row[0]).upper()
                    if "KUCOIN" in cell_value and "BTC" in cell_value:
                        self.currency_positions['BTC'] = row_idx + 1  # 1-based row
                        self.logger.info(f"Found BTC section at row {row_idx + 1}")
                    elif "KUCOIN" in cell_value and "ETH" in cell_value:
                        self.currency_positions['ETH'] = row_idx + 1  # 1-based row
                        self.logger.info(f"Found ETH section at row {row_idx + 1}")
            
            # Fallback to config if not found
            if 'BTC' not in self.currency_positions:
                self.currency_positions['BTC'] = Config.START_ROW
            if 'ETH' not in self.currency_positions:
                self.currency_positions['ETH'] = Config.START_ROW + 8  # Estimate
                
            self.logger.info(f"Currency positions: {self.currency_positions}")
            
        except Exception as e:
            self.logger.error(f"Error identifying sheet structure: {e}")
            # Use config defaults
            self.currency_positions = {
                'BTC': Config.START_ROW,
                'ETH': Config.START_ROW + 8
            }

    def calculate_premium(self, mark_price, index_price):
        """Calculate premium percentage and dollar premium"""
        if not mark_price or not index_price:
            return 0, 0
        dollar_premium = mark_price - index_price
        percent_premium = (dollar_premium / index_price) * 100
        return round(percent_premium, 2), round(dollar_premium, 2)

    def calculate_apr(self, mark_price, index_price, expiration_ms):
        """Calculate APR """
        if not index_price or expiration_ms <= 0:
            return 0.0
        minutes_left = (expiration_ms - datetime.now(timezone.utc).timestamp() * 1000) / 60000
        if minutes_left <= 0:
            return 0.0
        apr = ((mark_price / index_price) - 1) * 525600 / minutes_left * 100
        return round(apr, 2)

    def format_time_to_expiration(self, expiration_ms):
        """Convert expiration to total hours remaining"""
        if expiration_ms <= 0:
            return "-"
        minutes_left = int((expiration_ms - datetime.now(timezone.utc).timestamp() * 1000) / 60000)
        if minutes_left <= 0:
            return 0
        # Convert minutes to hours (decimal)
        hours_left = round(minutes_left / 60, 2)
        return hours_left

    async def get_websocket_token(self):
        """Get WebSocket token from KuCoin REST API"""
        try:
            response = requests.post(Config.WEBSOCKET_TOKEN_URL)
            data = response.json()
            
            if data.get('code') == '200000':
                self.ws_token = data['data']['token']
                self.ws_endpoint = data['data']['instanceServers'][0]['endpoint']
                self.logger.info("Successfully obtained WebSocket token")
                return True
            else:
                self.logger.error(f"Failed to get WebSocket token: {data}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error getting WebSocket token: {e}")
            return False

    async def connect_websocket(self):
        """Connect to KuCoin WebSocket"""
        try:
            if not await self.get_websocket_token():
                return False
            
            ws_url = f"{self.ws_endpoint}?token={self.ws_token}"
            self.logger.info(f"Connecting to KuCoin WebSocket: {ws_url}")
            
            self.websocket = await websockets.connect(ws_url)
            self.connected = True
            self.logger.info("Successfully connected to KuCoin WebSocket")
            return True
            
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            return False

    async def subscribe_to_instruments(self):
        """Subscribe to various data feeds for all instruments"""
        try:
            # Subscribe to symbol snapshots (24h statistics)
            for symbol in Config.TARGET_INSTRUMENTS:
                snapshot_sub = {
                    "id": f"snapshot_{symbol}",
                    "type": "subscribe",
                    "topic": f"/contractMarket/snapshot:{symbol}",
                    "response": True
                }
                await self.websocket.send(json.dumps(snapshot_sub))
                
                # Subscribe to instrument data (mark price, index price, funding rate)
                instrument_sub = {
                    "id": f"instrument_{symbol}",
                    "type": "subscribe",
                    "topic": f"/contract/instrument:{symbol}",
                    "response": True
                }
                await self.websocket.send(json.dumps(instrument_sub))
                
                # Subscribe to Level 5 orderbook for bid/ask
                orderbook_sub = {
                    "id": f"orderbook_{symbol}",
                    "type": "subscribe",
                    "topic": f"/contractMarket/level2Depth5:{symbol}",
                    "response": True
                }
                await self.websocket.send(json.dumps(orderbook_sub))
            
            self.logger.info(f"Sent subscriptions for {len(Config.TARGET_INSTRUMENTS)} instruments")
            
        except Exception as e:
            self.logger.error(f"Subscription failed: {e}")

    async def handle_message(self, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            
            if data.get('type') == 'welcome':
                self.logger.info("Received welcome message")
                await self.subscribe_to_instruments()
                return
                
            if data.get('type') == 'ack':
                self.logger.debug(f"Subscription acknowledged: {data.get('id')}")
                return
                
            if data.get('type') == 'message':
                topic = data.get('topic', '')
                
                # Extract symbol from topic
                symbol = None
                if '/contractMarket/snapshot:' in topic:
                    symbol = topic.split(':')[-1]
                    await self.handle_snapshot_data(symbol, data)
                elif '/contract/instrument:' in topic:
                    symbol = topic.split(':')[-1]
                    await self.handle_instrument_data(symbol, data)
                elif '/contractMarket/level2Depth5:' in topic:
                    symbol = topic.split(':')[-1]
                    await self.handle_orderbook_data(symbol, data)
                    
        except Exception as e:
            self.logger.error(f"Message handling error: {e}")

    async def handle_snapshot_data(self, symbol, data):
        """Handle symbol snapshot data (24h statistics)"""
        if symbol not in Config.TARGET_INSTRUMENTS:
            return
            
        try:
            with self.lock:
                if symbol not in self.instruments_data:
                    self.instruments_data[symbol] = {}
                    
                snapshot_data = data.get('data', {})
                self.instruments_data[symbol].update({
                    'lastPrice': snapshot_data.get('lastPrice'),
                    'volume': snapshot_data.get('volume', 0),
                    'turnover': snapshot_data.get('turnover', 0),
                    'priceChgPct': snapshot_data.get('priceChgPct', 0),
                    'lastUpdate': time.time()
                })
                
                self.last_update = time.time()
                
        except Exception as e:
            self.logger.error(f"Error handling snapshot data for {symbol}: {e}")

    async def handle_instrument_data(self, symbol, data):
        """Handle instrument data (mark price, index price, funding rate)"""
        if symbol not in Config.TARGET_INSTRUMENTS:
            return
            
        try:
            with self.lock:
                if symbol not in self.instruments_data:
                    self.instruments_data[symbol] = {}
                    
                subject = data.get('subject')
                instrument_data = data.get('data', {})
                
                if subject == 'mark.index.price':
                    self.instruments_data[symbol].update({
                        'markPrice': instrument_data.get('markPrice'),
                        'indexPrice': instrument_data.get('indexPrice'),
                        'lastUpdate': time.time()
                    })
                    
                elif subject == 'funding.rate':
                    self.instruments_data[symbol].update({
                        'fundingRate': instrument_data.get('fundingRate'),
                        'lastUpdate': time.time()
                    })
                
                self.last_update = time.time()
                
                # Log updates
                if subject == 'mark.index.price':
                    mark = instrument_data.get('markPrice', 0)
                    index = instrument_data.get('indexPrice', 0)
                    self.logger.debug(f"{symbol}: Mark=${mark:.2f}, Index=${index:.2f}")
                    
        except Exception as e:
            self.logger.error(f"Error handling instrument data for {symbol}: {e}")

    async def handle_orderbook_data(self, symbol, data):
        """Handle orderbook data (bid/ask prices)"""
        if symbol not in Config.TARGET_INSTRUMENTS:
            return
            
        try:
            with self.lock:
                if symbol not in self.instruments_data:
                    self.instruments_data[symbol] = {}
                    
                orderbook_data = data.get('data', {})
                bids = orderbook_data.get('bids', [])
                asks = orderbook_data.get('asks', [])
                
                if bids and asks:
                    self.instruments_data[symbol].update({
                        'bid': float(bids[0][0]),
                        'ask': float(asks[0][0]),
                        'lastUpdate': time.time()
                    })
                    
                self.last_update = time.time()
                
        except Exception as e:
            self.logger.error(f"Error handling orderbook data for {symbol}: {e}")

    def prepare_currency_data(self, currency, data_copy):
        """Prepare all data for one currency section"""
        currency_data = []
        
        # Get instruments for this currency
        if currency == 'BTC':
            instruments = Config.BTC_INSTRUMENTS
            perpetual_symbols = ['XBTUSDTM', 'XBTUSDM', 'XBTUSDCM', 'XBTMU25']
        else:  # ETH
            instruments = Config.ETH_INSTRUMENTS
            perpetual_symbols = ['ETHUSDTM', 'ETHUSDM', 'ETHUSDCM']
        
        for symbol in instruments:
            if symbol not in data_copy:
                self.logger.warning(f"No data available for {symbol}")
                continue
                
            data = data_copy[symbol]
            mark_price = data.get('markPrice')
            index_price = data.get('indexPrice')
            
            if not mark_price or not index_price:
                self.logger.warning(f"Skipping {symbol} - missing price data")
                continue
            
            # Calculate APR for futures contracts
            apr_value = 0
            contract_info = self.contracts_info.get(symbol, {})
            expiration_ms = contract_info.get('expireDate', 0)
            
            if expiration_ms and not symbol.endswith('TM'):  # Not perpetual
                apr_value = self.calculate_apr(mark_price, index_price, expiration_ms)
            
            # Calculate premium values
            percent_premium, dollar_premium = self.calculate_premium(mark_price, index_price)
            
            # Calculate settlement time
            settlement = self.format_time_to_expiration(expiration_ms) if expiration_ms else "-"
            
            # Calculate funding rate for perpetuals
            funding_rate = 0
            if symbol in perpetual_symbols:  # Check if symbol is in perpetual list
                rate = data.get('fundingRate', 0)
                if rate is not None:
                    # Convert to percentage and scale appropriately
                    funding_rate = round(float(rate) * 100, 4)
            
            currency_data.append({
                'A': apr_value,                           # APR %
                'B': symbol,                              # Instrument
                'C': float(data.get('bid', 0)),          # Bid Price
                'D': float(mark_price),                   # Mark Price
                'E': float(data.get('ask', 0)),          # Ask Price
                'F': percent_premium,                     # %Premium
                'G': dollar_premium,                      # $Premium
                'H': settlement,                          # Settlement
                'I': funding_rate                         # Funding Rate
            })
        
        return currency_data

    def setup_currency_structure(self, currency, start_row):
        """Setup headers for a currency section"""
        updates = []
        current_row = start_row
        
        # Currency header
        header = f'KUCOIN === {currency} Contracts ==='
        for col_idx in range(9):
            col_letter = Config.DATA_COLUMNS[col_idx]
            cell_value = header if col_idx == 0 else ''
            updates.append({
                'range': f'{col_letter}{current_row}',
                'values': [[cell_value]]
            })
        current_row += 1
        
        # Column headers
        headers = ['APR %', 'Instrument', 'Bid Price', 'Mark Price', 'Ask Price', '%Premium', '$Premium', 'Settlement', 'Funding Rate']
        for col_idx, header in enumerate(headers):
            col_letter = Config.DATA_COLUMNS[col_idx]
            updates.append({
                'range': f'{col_letter}{current_row}',
                'values': [[header]]
            })
        
        return updates, current_row + 1

    def update_sheet_safe(self):
        """Safe update approach: only touch columns A-I, use column-based ranges"""
        try:
            with self.lock:
                data_copy = json.loads(json.dumps(self.instruments_data))
            
            # Prepare all updates using column-based approach
            all_updates = []
            
            # Process each currency
            for currency in ['BTC', 'ETH']:
                currency_data = self.prepare_currency_data(currency, data_copy)
                if not currency_data:
                    self.logger.warning(f"No data for {currency}")
                    continue
                
                # Find or estimate starting position
                section_start = self.currency_positions.get(currency, Config.START_ROW)
                
                # Setup structure (headers)
                structure_updates, data_start_row = self.setup_currency_structure(currency, section_start)
                all_updates.extend(structure_updates)
                
                # Prepare column-wise updates for this currency
                for col_letter in Config.DATA_COLUMNS:
                    column_values = []
                    for row_data in currency_data:
                        column_values.append([row_data[col_letter]])
                    
                    if column_values:
                        end_row = data_start_row + len(column_values) - 1
                        range_name = f"{col_letter}{data_start_row}:{col_letter}{end_row}"
                        
                        all_updates.append({
                            'range': range_name,
                            'values': column_values
                        })
                
                self.logger.info(f"Prepared {len(currency_data)} {currency} instruments")
            
            # Execute batch update (only columns A-I, never touches formulas in J+)
            if all_updates:
                try:
                    self.worksheet.batch_update(all_updates)
                    total_cells = sum(len(update['values']) for update in all_updates if 'values' in update)
                    self.logger.info(f"Safe update: {total_cells} cells in columns A-I (formulas in J+ protected)")
                except Exception as e:
                    self.logger.error(f"Batch update failed: {e}")
                    return False
            else:
                self.logger.warning("No data to update")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Safe sheet update failed: {e}")
            return False

    async def update_loop(self):
        """Main update loop for Google Sheets"""
        while True:
            try:
                await asyncio.sleep(Config.UPDATE_INTERVAL)
                
                if time.time() - self.last_update < Config.UPDATE_INTERVAL * 3:
                    success = self.update_sheet_safe()
                    if success:
                        # Log current data status
                        with self.lock:
                            btc_count = len([s for s in Config.BTC_INSTRUMENTS if s in self.instruments_data])
                            eth_count = len([s for s in Config.ETH_INSTRUMENTS if s in self.instruments_data])
                            self.logger.info(f"Status: {btc_count}/{len(Config.BTC_INSTRUMENTS)} BTC, {eth_count}/{len(Config.ETH_INSTRUMENTS)} ETH instruments")
                    else:
                        self.logger.error("Update failed - will retry")
                else:
                    self.logger.warning(f"No recent data (last: {time.time() - self.last_update:.1f}s ago)")
                    
            except Exception as e:
                self.logger.error(f"Update loop error: {e}")
                await asyncio.sleep(10)

    async def websocket_loop(self):
        """Main WebSocket loop with reconnection"""
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                if not await self.connect_websocket():
                    retry_count += 1
                    await asyncio.sleep(5 * retry_count)
                    continue
                
                retry_count = 0
                
                async for message in self.websocket:
                    await self.handle_message(message)
                    
            except websockets.exceptions.ConnectionClosed:
                self.logger.warning("WebSocket connection closed, reconnecting...")
                retry_count += 1
                await asyncio.sleep(5 * retry_count)
            except Exception as e:
                self.logger.error(f"WebSocket error: {e}")
                retry_count += 1
                await asyncio.sleep(5 * retry_count)
        
        self.logger.error("Max retries reached")

    async def run(self):
        """Main application runner"""
        self.logger.info("Starting KuCoin WebSocket tracker")
        self.logger.info("Data columns: A-I | Protected: J+ (formulas safe)")
        
        # Run WebSocket and update loops concurrently
        await asyncio.gather(
            self.websocket_loop(),
            self.update_loop()
        )

if __name__ == "__main__":
    print("=" * 60)
    print("KuCoin WebSocket Tracker with Google Sheets")
    print("=" * 60)
    print("Target Instruments:")
    for instrument in Config.TARGET_INSTRUMENTS:
        print(f"  - {instrument}")
    print(f"Google Sheet ID: {Config.SHEET_ID}")
    print(f"Update Interval: {Config.UPDATE_INTERVAL} seconds")
    print("Data columns: A-I | Protected: J+ (formulas safe)")
    print("=" * 60)
    
    try:
        # Validate configuration
        if not os.path.exists(Config.CREDENTIALS_FILE):
            print(f"Error: Google credentials file not found: {Config.CREDENTIALS_FILE}")
            print("Please ensure you have the service account JSON file.")
            sys.exit(1)
        
        tracker = KuCoinWebSocketTracker()
        asyncio.run(tracker.run())
        
    except KeyboardInterrupt:
        print("\nTracker stopped by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)