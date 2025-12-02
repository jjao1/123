#!/usr/bin/env python3
"""
Pull.py is used to download the configuration from the remote server and extract the modules.
See spec: https://confluence.ztsystems.com/display/IMT/MFG+Test+Script+Deployment+Improvement+Arch+and+Plan

version: 1
init by: deping liang
date: 2025-11-10
"""

import os
import sys
import json
import subprocess
import shutil
import logging
import time
import hashlib
import urllib.request
import urllib.error
from pathlib import Path
from typing import Dict, Optional

# Configuration
LOG_FILE = '/var/log/pull.py.log'
MAX_RETRY_COUNT = 5
RETRY_INTERVAL = 10  # seconds
PULL_SH_PATH = '/usr/sbin/pull.sh'
DRY_RUN = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('pull')


class PullConfigManager:
    """Manages configuration retrieval and validation."""
    
    def __init__(self, repo_url: str):
        """
        Initialize config manager.
        
        Args:
            repo_url: Base repository URL
        """
        self.repo_url = repo_url.rstrip('/')
        self.config = None
    
    def get_next_server(self) -> Optional[str]:
        """Get next_server IP from DHCP."""
        try:
            result = subprocess.run(
                ['nmcli', '-f', 'DHCP4', 'device', 'show'],
                capture_output=True,
                text=True,
                check=True
            )
            for line in result.stdout.split('\n'):
                if 'next_server' in line:
                    return line.split()[-1]
            return None
        except Exception as e:
            logger.error(f"Failed to get next_server: {e}")
            return None
    
    def download_file(self, url: str, max_retries: int = MAX_RETRY_COUNT) -> Optional[bytes]:
        """
        Download file with retry mechanism.
        
        Args:
            url: URL to download
            max_retries: Maximum number of retry attempts
        
        Returns:
            File contents as bytes, or None if failed
        """
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Downloading {url} (attempt {attempt}/{max_retries})")
                with urllib.request.urlopen(url, timeout=30) as response:
                    return response.read()
            except urllib.error.URLError as e:
                logger.warning(f"Download failed (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    time.sleep(RETRY_INTERVAL)
                else:
                    logger.error(f"Failed to download {url} after {max_retries} attempts")
            except Exception as e:
                logger.error(f"Unexpected error downloading {url}: {e}")
                return None
        return None
    
    def calculate_md5(self, data: bytes) -> str:
        """Calculate MD5 hash of data."""
        return hashlib.md5(data).hexdigest()
    
    def retrieve_config(self, use_overwrite: bool = False, overwrite_url: Optional[str] = None) -> bool:
        """
        Retrieve and validate pull.json configuration.
        
        Args:
            use_overwrite: Whether to use overwrite URL
            overwrite_url: Overwrite URL endpoint
        
        Returns:
            True if successful, False otherwise
        """
        if use_overwrite and overwrite_url:
            config_url = overwrite_url
            logger.info(f"Using overwrite configuration URL: {config_url}")
        else:
            config_url = f"{self.repo_url}/pull.json"
            logger.info(f"Using default configuration URL: {config_url}")
        
        md5_url = config_url + ".md5"
        logger.info(f"MD5 checksum URL: {md5_url}")
        
        # Download MD5 first
        logger.info(f"Step 1: Downloading MD5 checksum file from {md5_url}")
        md5_data = self.download_file(md5_url)
        if not md5_data:
            logger.error("Failed to download pull.json.md5")
            return False
        
        expected_md5 = md5_data.decode('utf-8').strip()
        logger.info(f"Expected MD5 checksum: {expected_md5}")
        logger.info(f"MD5 file size: {len(md5_data)} bytes")
        
        # Download and validate config
        logger.info(f"Step 2: Downloading configuration file from {config_url}")
        for attempt in range(1, MAX_RETRY_COUNT + 1):
            config_data = self.download_file(config_url)
            if not config_data:
                continue
            
            logger.info(f"Configuration file downloaded: {len(config_data)} bytes")
            actual_md5 = self.calculate_md5(config_data)
            logger.info(f"Calculated MD5 checksum: {actual_md5}")
            logger.info(f"MD5 validation: {'PASS' if actual_md5 == expected_md5 else 'FAIL'}")
            
            if actual_md5 == expected_md5:
                try:
                    self.config = json.loads(config_data.decode('utf-8'))
                    logger.info("Configuration parsed successfully (stored in memory)")
                    logger.info(f"Configuration contains {len(self.config.get('modules', {}))} module(s)")
                    if 'overwrite' in self.config:
                        overwrite_status = self.config['overwrite'].get('overwrite', 'unknown')
                        logger.info(f"Overwrite configuration status: {overwrite_status}")
                    return True
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON configuration: {e}")
                    logger.error(f"JSON parse error at line {e.lineno}, column {e.colno}")
                    return False
            else:
                logger.warning(f"MD5 checksum mismatch (attempt {attempt}/{MAX_RETRY_COUNT})")
                logger.warning(f"Expected: {expected_md5}, Got: {actual_md5}")
                if attempt < MAX_RETRY_COUNT:
                    logger.info(f"Retrying in {RETRY_INTERVAL} seconds...")
                    time.sleep(RETRY_INTERVAL)
        
        logger.error("Failed to retrieve valid configuration after all retry attempts")
        return False


class ModuleExtractor:
    """Handles module extraction (integrated from pull.sh functionality)."""
    
    def __init__(self, modules: Dict, repo_url: str):
        """
        Initialize extractor.
        
        Args:
            modules: Module configuration dict
            repo_url: Repository URL for downloading modules (base URL: http://{next_server}/deployment/pull)
        """
        self.modules = modules
        self.repo_url = repo_url.rstrip('/')
        # Extract base URL: http://{next_server}/deployment
        # repo_url is http://{next_server}/deployment/pull
        # base_url should be http://{next_server}/deployment
        if self.repo_url.endswith('/pull'):
            self.base_url = self.repo_url[:-5].rstrip('/')  # Remove '/pull' and trailing slash
        else:
            # Fallback: assume repo_url is base URL
            self.base_url = self.repo_url.rstrip('/')
    
    def calculate_file_md5(self, file_path: str) -> Optional[str]:
        """
        Calculate MD5 hash of a file.
        
        Args:
            file_path: Path to the file
        
        Returns:
            MD5 hash as hex string, or None if failed
        """
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate MD5 for {file_path}: {e}")
            return None
    
    def verify_module_md5(self, file_path: str, expected_md5: Optional[str]) -> bool:
        """
        Verify MD5 checksum of a module file.
        
        Args:
            file_path: Path to the file to verify
            expected_md5: Expected MD5 checksum (from configuration)
        
        Returns:
            True if MD5 matches or if no expected MD5 provided, False otherwise
        """
        if not expected_md5:
            logger.info(f"  No MD5 checksum provided in configuration, skipping verification")
            return True
        
        if not os.path.exists(file_path):
            logger.error(f"  File does not exist for MD5 verification: {file_path}")
            return False
        
        logger.info(f"  Verifying MD5 checksum...")
        logger.info(f"    Expected MD5: {expected_md5}")
        
        actual_md5 = self.calculate_file_md5(file_path)
        if not actual_md5:
            logger.error(f"  Failed to calculate MD5 checksum")
            return False
        
        logger.info(f"    Calculated MD5: {actual_md5}")
        
        if actual_md5.lower() == expected_md5.lower():
            logger.info(f"    MD5 verification: PASS")
            return True
        else:
            logger.error(f"    MD5 verification: FAIL")
            logger.error(f"    Expected: {expected_md5}")
            logger.error(f"    Got: {actual_md5}")
            return False
    
    def download_module(self, filename: str, source_favors: str, expected_md5: Optional[str] = None, max_retries: int = 3) -> Optional[str]:
        """
        Download module file with optional MD5 verification.
        
        Args:
            filename: Name of file to download
            source_favors: Source favors (e.g., 'centos9') for URL construction
            expected_md5: Expected MD5 checksum for verification (optional)
            max_retries: Maximum retry attempts
        
        Returns:
            Local file path if successful, None otherwise
        """
        # URL format: http://{next_server}/deployment/{source_favors}/{filename}
        # Ensure no double slashes
        source_favors_clean = source_favors.strip('/')
        filename_clean = filename.lstrip('/')
        url = f"{self.base_url}/{source_favors_clean}/{filename_clean}"
        local_path = os.path.abspath(filename)
        
        logger.info(f"Module download details:")
        logger.info(f"  Source URL: {url}")
        logger.info(f"  Local save path: {local_path}")
        logger.info(f"  Source favors: {source_favors}")
        logger.info(f"  Filename: {filename}")
        logger.info(f"  Base URL: {self.base_url}")
        if expected_md5:
            logger.info(f"  Expected MD5: {expected_md5}")
        else:
            logger.info(f"  Expected MD5: Not provided (skipping verification)")
        
        # Check if file already exists
        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)
            logger.info(f"  File already exists at {local_path} ({file_size} bytes)")
            
            # Verify MD5 of existing file
            if expected_md5:
                if not self.verify_module_md5(local_path, expected_md5):
                    logger.warning(f"  Existing file MD5 verification failed, will re-download")
                    # Remove the file so it will be re-downloaded
                    try:
                        os.remove(local_path)
                        logger.info(f"  Removed existing file for re-download")
                    except Exception as e:
                        logger.error(f"  Failed to remove existing file: {e}")
                        return None
                else:
                    logger.info(f"  Existing file MD5 verification passed, skipping download")
                    return local_path
            else:
                logger.info(f"  Skipping download (file exists, no MD5 to verify)")
                return local_path
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Downloading module file (attempt {attempt}/{max_retries})")
                logger.info(f"  Source: {url}")
                logger.info(f"  Destination: {local_path}")
                
                data = urllib.request.urlopen(url, timeout=300).read()
                file_size = len(data)
                
                logger.info(f"  Downloaded: {file_size} bytes")
                logger.info(f"  Writing to file: {local_path}")
                
                with open(local_path, 'wb') as f:
                    f.write(data)
                
                # Verify file was written
                if not os.path.exists(local_path):
                    logger.error(f"  File was not created at {local_path}")
                    continue
                
                written_size = os.path.getsize(local_path)
                logger.info(f"  File saved successfully: {local_path}")
                logger.info(f"  File size on disk: {written_size} bytes")
                if written_size == file_size:
                    logger.info(f"  File size verification: PASS")
                else:
                    logger.warning(f"  File size verification: FAIL (expected {file_size}, got {written_size})")
                    continue
                
                # Verify MD5 checksum if provided
                if expected_md5:
                    if not self.verify_module_md5(local_path, expected_md5):
                        logger.warning(f"  MD5 verification failed (attempt {attempt}/{max_retries})")
                        # Remove the file and retry
                        try:
                            os.remove(local_path)
                            logger.info(f"  Removed file for retry")
                        except Exception as e:
                            logger.warning(f"  Failed to remove file: {e}")
                        
                        if attempt < max_retries:
                            logger.info(f"  Retrying download in {RETRY_INTERVAL} seconds...")
                            time.sleep(RETRY_INTERVAL)
                            continue
                        else:
                            logger.error(f"  MD5 verification failed after all retry attempts")
                            return None
                    else:
                        logger.info(f"  MD5 verification passed, file is valid")
                
                return local_path
            except Exception as e:
                logger.warning(f"Download failed (attempt {attempt}/{max_retries})")
                logger.warning(f"  URL: {url}")
                logger.warning(f"  Error: {e}")
                if attempt < max_retries:
                    logger.info(f"  Retrying in {RETRY_INTERVAL} seconds...")
                    time.sleep(RETRY_INTERVAL)
        
        logger.error(f"Failed to download {filename} after {max_retries} attempts")
        logger.error(f"  Final URL attempted: {url}")
        logger.error(f"  Target path: {local_path}")
        return None
    
    def extract_module(self, module_name: str, module_config: Dict) -> bool:
        """Extract a single module."""
        filename = module_config.get('filename')
        source_favors = module_config.get('source_favors', '')
        compress_tools = module_config.get('compress_tools', 'tar')
        compress_arg = module_config.get('compress_arg', '-xzvf')
        uncompress_to = module_config.get('uncompress_to', '-C /tmp')
        expected_md5 = module_config.get('md5', '').strip() if module_config.get('md5') else None
        
        logger.info(f"Processing module: {module_name}")
        logger.info(f"  Configuration:")
        logger.info(f"    filename: {filename}")
        logger.info(f"    source_favors: {source_favors}")
        logger.info(f"    compress_tools: {compress_tools}")
        logger.info(f"    compress_arg: {compress_arg}")
        logger.info(f"    uncompress_to: {uncompress_to}")
        if expected_md5:
            logger.info(f"    md5: {expected_md5}")
        else:
            logger.info(f"    md5: Not provided (MD5 verification will be skipped)")
        
        if not filename:
            logger.error(f"Module {module_name}: missing filename")
            return False
        
        if not source_favors:
            logger.error(f"Module {module_name}: missing source_favors")
            return False
        
        # Download module if not present
        archive_path = os.path.abspath(filename)
        logger.info(f"  Archive file path: {archive_path}")
        
        if not os.path.exists(archive_path):
            logger.info(f"  Archive not found locally, downloading...")
            archive_path = self.download_module(filename, source_favors, expected_md5)
            if not archive_path:
                logger.error(f"  Failed to download module {module_name}")
                return False
        else:
            file_size = os.path.getsize(archive_path)
            logger.info(f"  Archive found locally: {archive_path} ({file_size} bytes)")
            
            # Verify MD5 of existing file if provided
            if expected_md5:
                if not self.verify_module_md5(archive_path, expected_md5):
                    logger.error(f"  Existing file MD5 verification failed")
                    logger.info(f"  Attempting to re-download...")
                    try:
                        os.remove(archive_path)
                        logger.info(f"  Removed existing file")
                    except Exception as e:
                        logger.error(f"  Failed to remove existing file: {e}")
                        return False
                    
                    archive_path = self.download_module(filename, source_favors, expected_md5)
                    if not archive_path:
                        logger.error(f"  Failed to re-download module {module_name}")
                        return False
                else:
                    logger.info(f"  Existing file MD5 verification passed")
        
        # Final MD5 verification before extraction (if MD5 is provided)
        if expected_md5:
            logger.info(f"  Performing final MD5 verification before extraction...")
            if not self.verify_module_md5(archive_path, expected_md5):
                logger.error(f"  Final MD5 verification failed for {module_name}")
                logger.error(f"  Extraction will not proceed for module {module_name}")
                return False
            logger.info(f"  Final MD5 verification passed, proceeding with extraction")
        else:
            logger.info(f"  No MD5 provided in configuration, skipping final verification")
        
        # Extract based on tool
        # Command format: <compress_tools> <compress_arg> <uncompress_to> <filename>
        logger.info(f"  Preparing extraction command for {module_name}")
        logger.info(f"    Tool: {compress_tools}")
        logger.info(f"    Archive: {archive_path}")
        logger.info(f"    Target: {uncompress_to}")
        
        if compress_tools.lower() == 'tar':
            return self._extract_tar(archive_path, compress_arg, uncompress_to, module_name)
        elif compress_tools.lower() == 'zip':
            return self._extract_zip(archive_path, uncompress_to, module_name)
        else:
            logger.error(f"Unsupported compression tool: {compress_tools}")
            return False
    
    def _extract_tar(self, archive_path: str, compress_arg: str, uncompress_to: str, module_name: str) -> bool:
        """
        Extract tar archive.
        
        Command format: tar <compress_arg> <uncompress_to> <filename>
        Example: tar -xzvf -C /root/ ctcs.tar.gz
        """
        try:
            # Build command: tar <compress_arg> <uncompress_to> <filename>
            # compress_arg is like "-xzvf"
            # uncompress_to is like "-C /root/"
            cmd = ['tar'] + compress_arg.split() + [archive_path] + uncompress_to.split() 
            cmd_str = ' '.join(cmd)
            
            logger.info(f"  Extraction command: {cmd_str}")
            logger.info(f"  Command components:")
            logger.info(f"    Program: tar")
            logger.info(f"    Arguments: {compress_arg}")
            logger.info(f"    Target directory: {uncompress_to}")
            logger.info(f"    Archive file: {archive_path}")
            
            # Extract target directory for logging
            target_dir = None
            if '-C' in uncompress_to:
                parts = uncompress_to.split()
                if len(parts) >= 2 and parts[0] == '-C':
                    target_dir = parts[1]
                    logger.info(f"  Extracting to directory: {target_dir}")
                    if not os.path.exists(target_dir):
                        logger.info(f"  Target directory does not exist, will be created by tar")
            
            logger.info(f"  Executing extraction command...")
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            
            if result.returncode != 0:
                logger.error(f"  Extraction command failed with exit code: {result.returncode}")
                logger.error(f"  Command: {cmd_str}")
                logger.error(f"  Error output: {result.stderr}")
                if result.stdout:
                    logger.error(f"  Standard output: {result.stdout}")
                return False
            
            logger.info(f"  Extraction command completed successfully")
            if result.stdout:
                logger.info(f"  Command output: {result.stdout.strip()}")
            logger.info(f"  Successfully extracted {archive_path}")
            if target_dir and os.path.exists(target_dir):
                logger.info(f"  Verified target directory exists: {target_dir}")
            return True
        except Exception as e:
            logger.error(f"  Extraction error: {e}")
            logger.error(f"  Archive path: {archive_path}")
            logger.error(f"  Command: {cmd_str if 'cmd_str' in locals() else 'N/A'}")
            return False
    
    def _extract_zip(self, archive_path: str, uncompress_to: str, module_name: str) -> bool:
        """
        Extract zip archive.
        
        For zip, uncompress_to should contain the target directory.
        If it's in format "-C /path/", extract the path.
        """
        try:
            import zipfile
            # Extract target directory from uncompress_to
            # uncompress_to might be "-C /root/" or just "/root/"
            target_dir = uncompress_to.strip()
            if target_dir.startswith('-C'):
                # Extract directory after -C
                parts = target_dir.split()
                if len(parts) >= 2 and parts[0] == '-C':
                    target_dir = parts[1]
                else:
                    target_dir = '/tmp'  # Fallback
            else:
                # Remove leading/trailing slashes if needed
                target_dir = target_dir.strip('/')
                if not target_dir:
                    target_dir = '/tmp'
            
            logger.info(f"  Extraction method: zipfile (Python)")
            logger.info(f"  Archive file: {archive_path}")
            logger.info(f"  Target directory: {target_dir}")
            
            # Ensure target directory exists
            if not os.path.exists(target_dir):
                logger.info(f"  Creating target directory: {target_dir}")
                Path(target_dir).mkdir(parents=True, exist_ok=True)
            else:
                logger.info(f"  Target directory exists: {target_dir}")
            
            logger.info(f"  Opening zip archive: {archive_path}")
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                logger.info(f"  Archive contains {len(file_list)} file(s)")
                logger.info(f"  Extracting to: {target_dir}")
                zip_ref.extractall(target_dir)
            
            logger.info(f"  Successfully extracted {archive_path} to {target_dir}")
            logger.info(f"  Extracted {len(file_list)} file(s)")
            return True
        except Exception as e:
            logger.error(f"  Zip extraction failed: {e}")
            logger.error(f"  Archive path: {archive_path}")
            logger.error(f"  Target directory: {target_dir if 'target_dir' in locals() else 'N/A'}")
            return False
    
    def extract_all(self) -> bool:
        """Extract all modules."""
        if not self.modules:
            logger.warning("No modules to extract")
            return True
        
        logger.info("=" * 60)
        logger.info(f"Starting module extraction process")
        logger.info(f"Total modules to process: {len(self.modules)}")
        logger.info(f"Base URL for module downloads: {self.base_url}")
        logger.info("=" * 60)
        
        failed = []
        successful = []
        md5_verified = []
        md5_skipped = []
        md5_failed = []
        
        for idx, (module_name, module_config) in enumerate(self.modules.items(), 1):
            logger.info("")
            logger.info(f"[{idx}/{len(self.modules)}] Processing module: {module_name}")
            logger.info("-" * 60)
            
            # Check if MD5 is provided in config
            expected_md5 = module_config.get('md5', '').strip() if module_config.get('md5') else None
            if expected_md5:
                logger.info(f"  MD5 verification: ENABLED (expected: {expected_md5})")
            else:
                logger.warning(f"  MD5 verification: DISABLED (no MD5 in configuration)")
                md5_skipped.append(module_name)
            
            if self.extract_module(module_name, module_config):
                successful.append(module_name)
                if expected_md5:
                    md5_verified.append(module_name)
                logger.info(f"✓ Module {module_name} processed successfully")
            else:
                failed.append(module_name)
                if expected_md5:
                    md5_failed.append(module_name)
                logger.error(f"✗ Module {module_name} failed")
        
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Module extraction summary:")
        logger.info(f"  Total modules: {len(self.modules)}")
        logger.info(f"  Successful: {len(successful)}")
        logger.info(f"  Failed: {len(failed)}")
        logger.info("")
        logger.info(f"MD5 verification summary:")
        logger.info(f"  Modules with MD5 verification: {len(md5_verified) + len(md5_failed)}")
        logger.info(f"  MD5 verified successfully: {len(md5_verified)}")
        if md5_verified:
            logger.info(f"    Verified modules: {', '.join(md5_verified)}")
        if md5_failed:
            logger.error(f"  MD5 verification failed: {len(md5_failed)}")
            logger.error(f"    Failed modules: {', '.join(md5_failed)}")
        logger.info(f"  Modules without MD5 (skipped): {len(md5_skipped)}")
        if md5_skipped:
            logger.warning(f"    Skipped modules: {', '.join(md5_skipped)}")
        logger.info("")
        if successful:
            logger.info(f"  Successful modules: {', '.join(successful)}")
        if failed:
            logger.error(f"  Failed modules: {', '.join(failed)}")
        logger.info("=" * 60)
        
        if failed:
            return False
        
        logger.info("All modules extracted successfully")
        return True


def generate_pull_sh(modules: Dict, output_path: str = PULL_SH_PATH) -> bool:
    """
    Generate pull.sh script for compatibility/debugging.
    
    Command format: <compress_tools> <compress_arg> <uncompress_to> <filename>
    Example: tar -xzvf -C /root/ ctcs.tar.gz
    
    Args:
        modules: Module configuration
        output_path: Output file path
    
    Returns:
        True if successful
    """
    try:
        logger.info("Generating pull.sh script")
        logger.info(f"  Output path: {output_path}")
        logger.info(f"  Number of modules: {len(modules)}")
        
        commands = []
        with open(output_path, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("#pull.sh create by pull.py\n")
            f.write(f"#Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"#Total modules: {len(modules)}\n\n")
            
            for module_name, module_config in modules.items():
                filename = module_config.get('filename')
                compress_tools = module_config.get('compress_tools', 'tar')
                compress_arg = module_config.get('compress_arg', '-xzvf')
                uncompress_to = module_config.get('uncompress_to', '-C /tmp')
                
                # Command format: <compress_tools> <compress_arg> <uncompress_to> <filename>
                command = f"{compress_tools} {compress_arg} {uncompress_to} {filename}"
                commands.append((module_name, command))
                
                f.write(f"#modules - {module_name}\n")
                f.write(f"{command}\n")
                f.write("\n")
            
            f.write("exit 0\n")
        
        os.chmod(output_path, 0o755)
        file_size = os.path.getsize(output_path)
        
        logger.info(f"  Script generated successfully")
        logger.info(f"  File size: {file_size} bytes")
        logger.info(f"  Permissions set to: 755")
        logger.info(f"  Commands in script:")
        for module_name, command in commands:
            logger.info(f"    [{module_name}] {command}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to generate pull.sh: {e}")
        logger.error(f"  Output path: {output_path}")
        return False


def main():
    """Main entry point."""
    global DRY_RUN
    
    logger.info("=" * 60)
    logger.info("pull.py - Configuration Pull System")
    logger.info(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info("=" * 60)
    
    # Get repository URL from environment or DHCP
    logger.info("Step 1: Determining repository URL")
    repo_url = os.environ.get('REPO_URL')
    if repo_url:
        logger.info(f"  Repository URL from environment: {repo_url}")
    else:
        logger.info("  Repository URL not in environment, querying DHCP...")
        # Try to get from DHCP
        try:
            logger.info("  Executing: nmcli -f DHCP4 device show")
            result = subprocess.run(
                ['nmcli', '-f', 'DHCP4', 'device', 'show'],
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"  DHCP query successful")
            for line in result.stdout.split('\n'):
                if 'next_server' in line:
                    next_server = line.split()[-1]
                    repo_url = f"http://{next_server}/deployment/pull"
                    logger.info(f"  Found next_server from DHCP: {next_server}")
                    logger.info(f"  Constructed repository URL: {repo_url}")
                    break
        except Exception as e:
            logger.error(f"  Failed to determine repo URL from DHCP: {e}")
            return 1
    
    if not repo_url:
        logger.error("Repository URL not available")
        return 1
    
    logger.info(f"  Final repository URL: {repo_url}")
    logger.info("")
    
    # Retrieve configuration
    logger.info("Step 2: Retrieving configuration")
    config_manager = PullConfigManager(repo_url)
    if not config_manager.retrieve_config():
        logger.error("Failed to retrieve configuration")
        return 1
    
    config = config_manager.config
    logger.info("")
    
    # Check for overwrite configuration
    logger.info("Step 3: Checking for overwrite configuration")
    overwrite = config.get('overwrite', {})
    overwrite_status = overwrite.get('overwrite', '').lower()
    logger.info(f"  Overwrite status: {overwrite_status}")
    
    if overwrite_status in ['yes', 'true']:
        logger.info("  Overwrite configuration detected, retrieving from alternate source")
        overwrite_url = overwrite.get('url_endpoint', '').strip()
        logger.info(f"  Overwrite URL endpoint: {overwrite_url}")
        if overwrite_url:
            if not config_manager.retrieve_config(use_overwrite=True, overwrite_url=overwrite_url):
                logger.error("Failed to retrieve overwrite configuration")
                return 1
            config = config_manager.config
            logger.info("  Overwrite configuration retrieved successfully")
    else:
        logger.info("  Using default configuration (no overwrite)")
    logger.info("")
    
    # Get modules
    logger.info("Step 4: Processing module configuration")
    modules = config.get('modules', {})
    if not modules:
        logger.warning("No modules defined in configuration")
        return 0
    
    logger.info(f"  Found {len(modules)} module(s) in configuration:")
    for module_name, module_config in modules.items():
        filename = module_config.get('filename', 'N/A')
        source_favors = module_config.get('source_favors', 'N/A')
        uncompress_to = module_config.get('uncompress_to', 'N/A')
        md5 = module_config.get('md5', 'Not provided')
        logger.info(f"    - {module_name}: {filename}")
        logger.info(f"        Source: {source_favors}")
        logger.info(f"        Target: {uncompress_to}")
        logger.info(f"        MD5: {md5}")
    logger.info("")
    
    # Generate pull.sh for compatibility (always generate for debugging)
    logger.info("Step 5: Generating pull.sh script")
    if not generate_pull_sh(modules):
        logger.error("Failed to generate pull.sh script")
        return 1
    logger.info("")
    
    # Check dry-run mode
    logger.info("Step 6: Checking execution mode")
    DRY_RUN = os.environ.get('DRY_RUN', '').lower() in ['1', 'true', 'yes']
    
    if DRY_RUN:
        logger.info("  DRY-RUN MODE: Modules will not be extracted")
        logger.info("  Modules that would be extracted:")
        for module_name, module_config in modules.items():
            filename = module_config.get('filename')
            source_favors = module_config.get('source_favors')
            uncompress_to = module_config.get('uncompress_to')
            compress_tools = module_config.get('compress_tools', 'tar')
            compress_arg = module_config.get('compress_arg', '-xzvf')
            md5 = module_config.get('md5', 'Not provided')
            logger.info(f"    - {module_name}:")
            logger.info(f"        File: {filename}")
            logger.info(f"        Source URL: http://<next_server>/deployment/{source_favors}/{filename}")
            logger.info(f"        MD5: {md5}")
            logger.info(f"        Command: {compress_tools} {compress_arg} {uncompress_to} {filename}")
        logger.info("")
        logger.info("DRY-RUN completed successfully")
        return 0
    
    logger.info("  Normal execution mode: Modules will be extracted")
    logger.info("")
    
    # Extract modules directly (integrated approach)
    logger.info("Step 7: Extracting modules")
    extractor = ModuleExtractor(modules, repo_url)
    if not extractor.extract_all():
        logger.error("Module extraction failed")
        return 1
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("All operations completed successfully")
    logger.info(f"Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    return 0


if __name__ == '__main__':
    sys.exit(main())

