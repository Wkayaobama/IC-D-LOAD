#!/usr/bin/env python3
"""
SSH PostgreSQL Connection Manager
==================================

This module provides SSH-based PostgreSQL connections using paramiko.
It connects to PostgreSQL through an SSH tunnel and can execute commands as sudo.

Usage:
    from ssh_postgres_manager import SSHPostgreSQLManager

    # Connect via SSH
    ssh_pg = SSHPostgreSQLManager(
        ssh_host="your-server.com",
        ssh_user="ubuntu",
        ssh_key_path="~/.ssh/id_rsa",
        pg_host="localhost",
        pg_database="postgres"
    )

    # Execute SQL
    results = ssh_pg.execute_query("SELECT * FROM my_table LIMIT 10")

    # Execute SQL file
    ssh_pg.execute_sql_file("schema.sql")

    # Execute as sudo
    ssh_pg.execute_remote_command("psql -U postgres -c 'CREATE DATABASE mydb'", sudo=True)
"""

import paramiko
import io
import time
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from contextlib import contextmanager
import pandas as pd
from loguru import logger


class SSHPostgreSQLManager:
    """
    Manages PostgreSQL connections over SSH with sudo support.

    Features:
    - SSH tunnel to PostgreSQL
    - Execute SQL via SSH
    - Execute commands as sudo
    - File transfer (SCP)
    - SQL file execution
    """

    def __init__(
        self,
        ssh_host: str,
        ssh_user: str,
        ssh_port: int = 22,
        ssh_password: Optional[str] = None,
        ssh_key_path: Optional[str] = None,
        pg_host: str = "localhost",
        pg_port: int = 5432,
        pg_database: str = "postgres",
        pg_user: str = "postgres",
        pg_password: Optional[str] = None,
        sudo_password: Optional[str] = None
    ):
        """
        Initialize SSH PostgreSQL manager.

        Args:
            ssh_host: SSH server hostname
            ssh_user: SSH username
            ssh_port: SSH port (default: 22)
            ssh_password: SSH password (if not using key)
            ssh_key_path: Path to SSH private key
            pg_host: PostgreSQL host (as seen from SSH server)
            pg_port: PostgreSQL port
            pg_database: Database name
            pg_user: PostgreSQL username
            pg_password: PostgreSQL password
            sudo_password: Password for sudo commands
        """
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_port = ssh_port
        self.ssh_password = ssh_password
        self.ssh_key_path = Path(ssh_key_path).expanduser() if ssh_key_path else None

        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_database = pg_database
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.sudo_password = sudo_password

        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.sftp_client: Optional[paramiko.SFTPClient] = None

    def connect(self):
        """
        Establish SSH connection.
        """
        logger.info(f"Connecting to SSH server: {self.ssh_user}@{self.ssh_host}:{self.ssh_port}")

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Connect with key or password
            if self.ssh_key_path and self.ssh_key_path.exists():
                logger.info(f"Using SSH key: {self.ssh_key_path}")
                self.ssh_client.connect(
                    hostname=self.ssh_host,
                    port=self.ssh_port,
                    username=self.ssh_user,
                    key_filename=str(self.ssh_key_path),
                    timeout=30
                )
            elif self.ssh_password:
                logger.info("Using SSH password authentication")
                self.ssh_client.connect(
                    hostname=self.ssh_host,
                    port=self.ssh_port,
                    username=self.ssh_user,
                    password=self.ssh_password,
                    timeout=30
                )
            else:
                raise ValueError("Either ssh_key_path or ssh_password must be provided")

            logger.info("✓ SSH connection established")

            # Create SFTP client
            self.sftp_client = self.ssh_client.open_sftp()
            logger.info("✓ SFTP client ready")

        except Exception as e:
            logger.error(f"✗ SSH connection failed: {e}")
            raise

    def disconnect(self):
        """
        Close SSH and SFTP connections.
        """
        if self.sftp_client:
            self.sftp_client.close()
            logger.info("✓ SFTP client closed")

        if self.ssh_client:
            self.ssh_client.close()
            logger.info("✓ SSH connection closed")

    def execute_remote_command(
        self,
        command: str,
        sudo: bool = False,
        retry: int = 3
    ) -> Tuple[str, str, int]:
        """
        Execute a command on the remote server.

        Args:
            command: Command to execute
            sudo: If True, execute with sudo
            retry: Number of retry attempts

        Returns:
            Tuple of (stdout, stderr, exit_code)
        """
        if not self.ssh_client:
            self.connect()

        if sudo:
            if not self.sudo_password:
                raise ValueError("sudo_password required for sudo commands")
            # Use sudo with password
            command = f"echo '{self.sudo_password}' | sudo -S {command}"

        attempt = 0
        last_error = None

        while attempt < retry:
            try:
                logger.info(f"Executing command: {command[:100]}{'...' if len(command) > 100 else ''}")

                stdin, stdout, stderr = self.ssh_client.exec_command(command)

                # Wait for command to complete
                exit_code = stdout.channel.recv_exit_status()

                stdout_text = stdout.read().decode('utf-8')
                stderr_text = stderr.read().decode('utf-8')

                if exit_code == 0:
                    logger.info(f"✓ Command executed successfully")
                    return stdout_text, stderr_text, exit_code
                else:
                    logger.warning(f"⚠ Command exited with code {exit_code}")
                    logger.warning(f"stderr: {stderr_text}")
                    return stdout_text, stderr_text, exit_code

            except Exception as e:
                attempt += 1
                last_error = e
                logger.warning(f"✗ Command attempt {attempt}/{retry} failed: {e}")

                if attempt < retry:
                    time.sleep(2 ** attempt)  # Exponential backoff

        logger.error(f"✗ Command failed after {retry} attempts: {last_error}")
        raise last_error

    def execute_query(
        self,
        query: str,
        fetch: bool = True,
        retry: int = 3
    ) -> Optional[List[Dict]]:
        """
        Execute a PostgreSQL query via SSH.

        Args:
            query: SQL query
            fetch: If True, fetch and return results
            retry: Number of retry attempts

        Returns:
            List of rows as dictionaries (if fetch=True)
        """
        # Build psql command
        psql_command = f"psql -h {self.pg_host} -p {self.pg_port} -U {self.pg_user} -d {self.pg_database}"

        # Add password if provided
        if self.pg_password:
            psql_command = f"PGPASSWORD='{self.pg_password}' {psql_command}"

        # Add query options
        if fetch:
            # Output as CSV for easy parsing
            psql_command += " -t -A -F ',' -c"
        else:
            psql_command += " -c"

        # Execute query
        full_command = f"{psql_command} \"{query}\""

        stdout, stderr, exit_code = self.execute_remote_command(full_command, retry=retry)

        if exit_code != 0:
            raise Exception(f"Query failed: {stderr}")

        if not fetch:
            logger.info("✓ Query executed successfully (no fetch)")
            return None

        # Parse CSV output
        if stdout.strip():
            # Split by lines
            lines = stdout.strip().split('\n')

            # First line should be headers (if we include them)
            # For now, we're using -t (no headers), so we need to get column names separately
            logger.info(f"✓ Query returned {len(lines)} rows")

            # Return raw lines for now
            # TODO: Parse into dictionaries if column names are needed
            return [{"data": line} for line in lines]
        else:
            logger.info("✓ Query returned 0 rows")
            return []

    def execute_query_df(
        self,
        query: str,
        retry: int = 3
    ) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame.

        Args:
            query: SQL query
            retry: Number of retry attempts

        Returns:
            pandas DataFrame
        """
        # Build psql command to output CSV with headers
        psql_command = f"psql -h {self.pg_host} -p {self.pg_port} -U {self.pg_user} -d {self.pg_database}"

        if self.pg_password:
            psql_command = f"PGPASSWORD='{self.pg_password}' {psql_command}"

        # Output as CSV with headers
        psql_command += " -c"

        full_command = f"{psql_command} \"\\copy ({query}) TO STDOUT WITH CSV HEADER\""

        stdout, stderr, exit_code = self.execute_remote_command(full_command, retry=retry)

        if exit_code != 0:
            raise Exception(f"Query failed: {stderr}")

        if stdout.strip():
            # Parse CSV
            df = pd.read_csv(io.StringIO(stdout))
            logger.info(f"✓ Query returned DataFrame with {len(df)} rows, {len(df.columns)} columns")
            return df
        else:
            logger.info("✓ Query returned empty DataFrame")
            return pd.DataFrame()

    def upload_file(self, local_path: str, remote_path: str):
        """
        Upload a file to the remote server via SFTP.

        Args:
            local_path: Local file path
            remote_path: Remote file path
        """
        if not self.sftp_client:
            self.connect()

        logger.info(f"Uploading {local_path} → {remote_path}")

        try:
            self.sftp_client.put(local_path, remote_path)
            logger.info(f"✓ File uploaded successfully")
        except Exception as e:
            logger.error(f"✗ File upload failed: {e}")
            raise

    def download_file(self, remote_path: str, local_path: str):
        """
        Download a file from the remote server via SFTP.

        Args:
            remote_path: Remote file path
            local_path: Local file path
        """
        if not self.sftp_client:
            self.connect()

        logger.info(f"Downloading {remote_path} → {local_path}")

        try:
            self.sftp_client.get(remote_path, local_path)
            logger.info(f"✓ File downloaded successfully")
        except Exception as e:
            logger.error(f"✗ File download failed: {e}")
            raise

    def execute_sql_file(
        self,
        local_sql_path: str,
        remote_temp_path: str = "/tmp/temp_query.sql",
        sudo: bool = False
    ):
        """
        Upload and execute a SQL file on the remote server.

        Args:
            local_sql_path: Local path to SQL file
            remote_temp_path: Temporary path on remote server
            sudo: If True, execute with sudo
        """
        # Upload SQL file
        self.upload_file(local_sql_path, remote_temp_path)

        # Build psql command
        psql_command = f"psql -h {self.pg_host} -p {self.pg_port} -U {self.pg_user} -d {self.pg_database} -f {remote_temp_path}"

        if self.pg_password:
            psql_command = f"PGPASSWORD='{self.pg_password}' {psql_command}"

        # Execute
        stdout, stderr, exit_code = self.execute_remote_command(psql_command, sudo=sudo)

        if exit_code != 0:
            logger.error(f"✗ SQL file execution failed: {stderr}")
            raise Exception(f"SQL file execution failed: {stderr}")

        logger.info(f"✓ SQL file executed successfully")
        logger.info(f"Output:\n{stdout}")

        # Clean up temp file
        self.execute_remote_command(f"rm {remote_temp_path}")

    def test_connection(self) -> bool:
        """
        Test the SSH and PostgreSQL connection.

        Returns:
            True if both connections successful
        """
        try:
            # Test SSH
            if not self.ssh_client:
                self.connect()

            stdout, stderr, exit_code = self.execute_remote_command("echo 'SSH connection OK'")
            logger.info(f"✓ SSH test: {stdout.strip()}")

            # Test PostgreSQL
            df = self.execute_query_df("SELECT version();")
            logger.info(f"✓ PostgreSQL test successful")
            logger.info(f"  Version: {df.iloc[0, 0][:80]}...")

            return True

        except Exception as e:
            logger.error(f"✗ Connection test failed: {e}")
            return False

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    import sys

    print("SSH PostgreSQL Manager Test")
    print("=" * 80)

    # Configuration (update these values)
    config = {
        "ssh_host": "your-server.com",
        "ssh_user": "ubuntu",
        "ssh_key_path": "~/.ssh/id_rsa",
        # "ssh_password": "your_ssh_password",  # Alternative to key
        "pg_host": "localhost",
        "pg_port": 5432,
        "pg_database": "postgres",
        "pg_user": "postgres",
        "pg_password": "your_pg_password",
        # "sudo_password": "your_sudo_password",  # For sudo commands
    }

    print("\nConfiguration:")
    for key, value in config.items():
        if "password" in key:
            print(f"  {key}: {'*' * 8}")
        else:
            print(f"  {key}: {value}")

    print("\n" + "=" * 80)
    print("Update the configuration above and run this script to test the connection")
    print("=" * 80)

    # Uncomment to test:
    # with SSHPostgreSQLManager(**config) as ssh_pg:
    #     # Test connection
    #     if ssh_pg.test_connection():
    #         print("\n✓ Connection successful!")
    #
    #         # Example query
    #         print("\nExecuting test query...")
    #         df = ssh_pg.execute_query_df("SELECT current_database(), current_user;")
    #         print(df)
    #
    #         # Example command execution
    #         print("\nExecuting remote command...")
    #         stdout, stderr, exit_code = ssh_pg.execute_remote_command("whoami")
    #         print(f"Current user: {stdout.strip()}")
