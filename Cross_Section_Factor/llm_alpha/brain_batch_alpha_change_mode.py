"""WorldQuant Brain API Batch processing module"""

import json
import os
from datetime import datetime
from os.path import expanduser
from time import sleep
import types

import requests
from requests.auth import HTTPBasicAuth

# from alpha_strategy import AlphaStrategy
# from dataset_config import get_dataset_config


class BrainBatchAlpha:
    API_BASE_URL = 'https://api.worldquantbrain.com'

    def __init__(self, credentials_file='brain_credentials.txt'):
        """Initialize API client"""

        self.session = requests.Session()
        self.last_status_code = None
        self._setup_authentication(credentials_file)

    def _setup_authentication(self, credentials_file):
        """Set up authentication"""

        try:
            with open(expanduser(credentials_file)) as f:
                credentials = json.load(f)
            username, password = credentials
            self.session.auth = HTTPBasicAuth(username, password)

            response = self.session.post(f"{self.API_BASE_URL}/authentication")
            if response.status_code not in [200, 201]:
                raise Exception(f"Authentication failed: HTTP{response.status_code}")

            print("✅ Authentication successful!")

        except Exception as e:
            print(f"❌ Authentication error:{str(e)}")
            raise

    def reauthenticate(self, credentials_file='brain_credentials.txt'):
        self._setup_authentication(credentials_file)
        
    def simulate_alphas(self, datafields=None, strategy_mode=1, dataset_name=None):
        """Simulation Alpha List"""

        try:
            datafields = self._get_datafields_if_none(datafields, dataset_name)
            if not datafields:
                return []

            alpha_list = self._generate_alpha_list(datafields, strategy_mode)
            if not alpha_list:
                return []

            print(f"\n🚀 Start simulation{len(alpha_list)} Alpha expressions...")

            results = []
            for i, alpha in enumerate(alpha_list, 1):
                print(f"\n[{i}/{len(alpha_list)}] Simulating Alpha...")
                result = self._simulate_single_alpha(alpha)
                if result and result.get('passed_all_checks'):
                    results.append(result)
                    self._save_alpha_id(result['alpha_id'], result)

                if i < len(alpha_list):
                    sleep(5)

            return results

        except Exception as e:
            print(f"❌ An error occurred during simulation:{str(e)}")
            return []

    def _simulate_single_alpha(self, alpha, max_retries=3, max_simulation_time=180):
        """Simulate a single Alpha"""
        for attempt in range(max_retries):
            try:
                print(f"expression:{alpha.get('regular', 'Unknown')}")
                print(f"Number of attempts:{attempt + 1}/{max_retries}")

                # Send mock request
                sim_resp = self.session.post(
                    f"{self.API_BASE_URL}/simulations",
                    json=alpha,
                    timeout=30  # Add request timeout
                )

                self.last_status_code = sim_resp.status_code

                if sim_resp.status_code != 201:
                    if sim_resp.status_code in [401, 403]:
                        print("⚠️ Authentication has expired, try to re-authenticate...")
                        self.reauthenticate()
                        continue
                    print(f"❌ Impersonation request failed (status code:{sim_resp.status_code})")
                    if attempt < max_retries - 1:
                        print("Wait 5 seconds and try again...")
                        sleep(5)
                        continue
                    return None

                # Get and verify progress URL
                if 'Location' not in sim_resp.headers:
                    print("❌ Location field missing in response header")
                    if attempt < max_retries - 1:
                        sleep(5)
                        continue
                    return None

                sim_progress_url = sim_resp.headers['Location']
                start_time = datetime.now()
                last_progress_time = start_time  # Record the time of the last output progress

                # Loop to check simulation progress
                while True:
                    # Check if timed out
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed > max_simulation_time:
                        print(f"⚠️ simulation timeout ({max_simulation_time}Second)")
                        break

                    try:
                        sim_progress_resp = self.session.get(
                            sim_progress_url,
                            timeout=30
                        )
                    except requests.exceptions.RequestException as e:
                        print(f"⚠️ Error while getting progress:{str(e)}")
                        sleep(5)
                        continue

                    if sim_progress_resp.status_code != 200:
                        print(f"❌ Failed to get progress (status code:{sim_progress_resp.status_code})")
                        if sim_progress_resp.status_code in [401, 403]:
                            self.reauthenticate()
                        break

                    retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))

                    if retry_after_sec == 0:  # Simulation completed
                        try:
                            response_data = sim_progress_resp.json()
                            if 'alpha' not in response_data:
                                raise KeyError("Alpha ID missing from response")
                                
                            alpha_id = response_data['alpha']
                            print(f"✅ Get Alpha ID:{alpha_id}")

                            # Get Alpha details
                            sleep(3)  # Wait for indicator calculation to complete
                            alpha_detail = self.session.get(
                                f"{self.API_BASE_URL}/alphas/{alpha_id}",
                                timeout=30
                            )
                            
                            if alpha_detail.status_code != 200:
                                raise requests.exceptions.RequestException(
                                    f"Failed to obtain Alpha details (status code:{alpha_detail.status_code})"
                                )
                                
                            alpha_data = alpha_detail.json()
                            if 'is' not in alpha_data:
                                raise KeyError("Unable to get indicator data")

                            is_qualified = self.check_alpha_qualification(alpha_data)

                            return {
                                'expression': alpha.get('regular'),
                                'alpha_id': alpha_id,
                                'passed_all_checks': is_qualified,
                                'metrics': alpha_data.get('is', {}),
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            
                        except (KeyError, json.JSONDecodeError, requests.exceptions.RequestException) as e:
                            print(f"❌ Error while processing simulation results:{str(e)}")
                            break

                    # Check whether progress information needs to be output (once every 30 seconds)
                    current_time = datetime.now()
                    time_since_last_progress = (current_time - last_progress_time).total_seconds()
                    
                    if time_since_last_progress >= 30:  # Output every 30 seconds
                        progress = min(95, (elapsed / 60) * 100)
                        print(f"⏳ Waiting for simulation results... ({elapsed:.1f} Second| The progress is approx.{progress:.0f}%)")
                        last_progress_time = current_time
                    
                    sleep(retry_after_sec)

            except Exception as e:
                print(f"⚠️ Exception occurred:{str(e)}")
                if attempt < max_retries - 1:
                    sleep(5)
                    continue

        print("❌ Maximum number of retries reached, simulation failed")
        return None

    def check_alpha_qualification(self, alpha_data):
        """Check if Alpha meets all commit conditions"""

        try:
            # from'is' Field acquisition indicator
            is_data = alpha_data.get('is', {})
            if not is_data:
                print("❌ Unable to get indicator data")
                return False

            # Get indicator value
            sharpe = float(is_data.get('sharpe', 0))
            fitness = float(is_data.get('fitness', 0))
            turnover = float(is_data.get('turnover', 0))
            ic_mean = float(is_data.get('margin', 0))  # margin Corresponding to IC Mean

            # Get subuniverse Sharpe
            sub_universe_check = next(
                (
                    check for check in is_data.get('checks', [])
                    if check['name'] == 'LOW_SUB_UNIVERSE_SHARPE'
                ),
                {}
            )
            subuniverse_sharpe = float(sub_universe_check.get('value', 0))
            required_subuniverse_sharpe = float(sub_universe_check.get('limit', 0))

            # # Print indicators
            # print("\n📊 Alpha Indicator details:")
            # print(f"  Sharpe: {sharpe:.3f} (>1.5)")
            # print(f"  Fitness: {fitness:.3f} (>1.0)")
            # print(f"  Turnover: {turnover:.3f} (0.1-0.9)")
            # print(f"  IC Mean: {ic_mean:.3f} (>0.02)")
            # print(f"  Subverse Sharpe:{subuniverse_sharpe:.3f} (>{required_subuniverse_sharpe:.3f})")

            # print("\n📝 Indicator evaluation results:")

            # Check each metric and output the results
            is_qualified = True

            if sharpe < 1.5:
                # print("❌ Sharpe ratio Not up to standard")
                is_qualified = False
            # else:
                # print("✅ Sharpe ratio Meet the standard")

            if fitness < 1.0:
                # print("❌ Fitness Not up to standard")
                is_qualified = False
            # else:
                # print("✅ Fitness Meet the standard")

            if turnover < 0.1 or turnover > 0.9:
                # print("❌ Turnover Not within the reasonable range")
                is_qualified = False
            # else:
                # print("✅ Turnover Meet the standard")

            # if ic_mean < 0.02:
                # print("❌ IC Mean Not up to standard")
                # is_qualified = False
            # else:
                # print("✅ IC Mean Meet the standard")

            if subuniverse_sharpe < required_subuniverse_sharpe:
                # print(f"❌ Subuniverse Sharpe is not up to standard ({subuniverse_sharpe:.3f} < {required_subuniverse_sharpe:.3f})")
                is_qualified = False
            # else:
                # print(f"✅ Subuniverse Sharpe reaches the standard ({subuniverse_sharpe:.3f} > {required_subuniverse_sharpe:.3f})")

            # print("\n🔍 Check item results:")
            checks = is_data.get('checks', [])
            for check in checks:
                name = check.get('name')
                result = check.get('result')
                value = check.get('value', 'N/A')
                limit = check.get('limit', 'N/A')

                if result == 'PASS':
                    # print(f"✅ {name}: {value} (limit:{limit})")
                    pass
                elif result == 'FAIL':
                    # print(f"❌ {name}: {value} (limit:{limit})")
                    is_qualified = False
                # elif result == 'PENDING':
                    # print(f"⚠️ {name}: Check not yet completed")
                    # is_qualified = False

            # print("\n📋 Final judgment:")
            # if is_qualified:
            #     print("✅ Alpha All conditions are met and you can submit!")
            # else:
            #     print("❌ Alpha Submission criteria not met")

            return is_qualified

        except Exception as e:
            print(f"❌ Error checking Alpha eligibility:{str(e)}")
            return False

    def submit_alpha(self, alpha_id):
        """Submit a single Alpha"""

        submit_url = f"{self.API_BASE_URL}/alphas/{alpha_id}/submit"

        for attempt in range(5):
            print(f"🔄 No.{attempt + 1} Attempts to Submit Alpha{alpha_id}")

            # POST ask
            res = self.session.post(submit_url)
            if res.status_code == 201:
                print("✅ POST: Successful, waiting for submission to complete...")
            elif res.status_code in [400, 403]:
                print(f"❌ Submission rejected ({res.status_code})")
                return False
            else:
                sleep(3)
                continue

            # Check submission status
            while True:
                res = self.session.get(submit_url)
                retry = float(res.headers.get('Retry-After', 0))

                if retry == 0:
                    if res.status_code == 200:
                        print("✅ Submission successful!")
                        return True
                    return False

                sleep(retry)

        return False

    def submit_multiple_alphas(self, alpha_ids):
        """Batch Submit Alpha"""
        successful = []
        failed = []

        for alpha_id in alpha_ids:
            if self.submit_alpha(alpha_id):
                successful.append(alpha_id)
            else:
                failed.append(alpha_id)

            if alpha_id != alpha_ids[-1]:
                sleep(10)

        return successful, failed
