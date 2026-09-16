from abc import ABC, abstractmethod


class BaseHDFSClient(ABC):
    """
    Abstract base class cho HDFS client
    """

    @abstractmethod
    def read(self, hdfs_path: str) -> str:
        """Read file content"""
        pass

    @abstractmethod
    def upload(self, hdfs_path: str, local_file: str):
        """Upload single file"""
        pass

    @abstractmethod
    def upload_dir(self, local_dir: str, hdfs_dir: str):
        """Upload directory recursively"""
        pass

    @abstractmethod
    def mkdirs(self, hdfs_path: str):
        """Create directory"""
        pass

    @abstractmethod
    def remove(self, hdfs_path: str):
        """Delete path recursively"""
        pass

    @abstractmethod
    def replace(self, hdfs_path: str, local_file: str):
        """Replace file or directory"""
        pass

    @abstractmethod
    def exists(self, hdfs_path: str) -> bool:
        """Check if HDFS path exists"""
        pass
