import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Generator, Set
from pyicloud import PyiCloudService
from pyicloud.services.photos import PhotoAsset, PhotoAlbum

from src.config import ICloudConfig

logger = logging.getLogger("immich_sync.icloud")

class ICloudClient:
    def __init__(self, config: ICloudConfig):
        self.apple_id = config.apple_id
        self.password = config.password
        self.session_dir = Path(config.session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.api: Optional[PyiCloudService] = None

    def connect(self, interactive_2fa: bool = False) -> PyiCloudService:
        """Connect to iCloud and authenticate, handling 2FA if needed."""
        self.api = PyiCloudService(
            apple_id=self.apple_id,
            password=self.password,
            cookie_directory=str(self.session_dir)
        )

        if self.api.requires_2fa:
            if not interactive_2fa:
                raise RuntimeError(
                    "iCloud requires Two-Factor Authentication (2FA). "
                    "Please run 'python sync.py login-icloud' in your terminal to complete verification."
                )

            print("\n[iCloud 2FA Verification Required]")
            code = input("Enter the 6-digit verification code sent to your Apple devices: ").strip()
            result = self.api.validate_2fa_code(code)
            if not result:
                raise RuntimeError("Failed to verify iCloud 2FA code. Please check code and try again.")
            
            if not self.api.is_trusted_session:
                print("Requesting trusted session...")
                res = self.api.trust_session()
                if not res:
                    print("Warning: Could not establish trusted session; 2FA may be prompted on subsequent runs.")
                else:
                    print("iCloud session trusted and cached successfully.")

        return self.api

    @property
    def photos(self):
        if not self.api:
            self.connect()
        return self.api.photos

    def iter_all_photos(self, limit: Optional[int] = None) -> Generator[PhotoAsset, None, None]:
        """Iterate through all photos in iCloud library."""
        count = 0
        for photo in self.photos.all:
            yield photo
            count += 1
            if limit and count >= limit:
                break

    def download_photo(self, photo: PhotoAsset, dest_path: Path) -> Path:
        """Download original photo asset to destination path."""
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        download = photo.download()
        with open(dest_path, "wb") as f:
            for chunk in download.iter_content(chunk_size=65536):
                f.write(chunk)
        return dest_path

    def upload_photo(self, file_path: Path) -> Optional[str]:
        """
        Upload photo asset into iCloud Photos library.
        Returns the created asset ID or None.
        """
        try:
            asset = self.photos.upload(str(file_path))
            if asset:
                return getattr(asset, "id", None) or getattr(asset, "asset_id", None)
        except Exception as e:
            logger.error(f"Failed to upload photo {file_path.name} to iCloud: {e}")
        return None

    def delete_photo(self, photo: PhotoAsset) -> bool:
        """
        Delete photo from iCloud Photos library (moves to Recently Deleted).
        """
        try:
            photo.delete()
            return True
        except Exception as e:
            logger.error(f"Failed to delete iCloud photo {getattr(photo, 'id', 'unknown')}: {e}")
            return False

    def find_photos_by_ids(self, asset_ids: Set[str]) -> Dict[str, PhotoAsset]:
        """Search multiple photos by their asset IDs in a single pass through iCloud library."""
        found: Dict[str, PhotoAsset] = {}
        if not asset_ids:
            return found
        target_ids = set(asset_ids)
        for photo in self.photos.all:
            pid = getattr(photo, "id", None) or getattr(photo, "asset_id", None)
            if pid in target_ids:
                found[pid] = photo
                if len(found) == len(target_ids):
                    break
        return found

    def find_photo_by_id(self, asset_id: str) -> Optional[PhotoAsset]:
        """Search photo by asset ID in iCloud library."""
        if not asset_id:
            return None
        res = self.find_photos_by_ids({asset_id})
        return res.get(asset_id)

    def get_albums(self) -> Dict[str, PhotoAlbum]:
        """Return dict of available albums in iCloud."""
        return self.photos.albums

    def create_album(self, album_name: str) -> Optional[PhotoAlbum]:
        """Create an album in iCloud Photos if supported."""
        try:
            return self.photos.create_album(album_name)
        except Exception as e:
            logger.warning(f"Could not create iCloud album '{album_name}': {e}")
            return None

    def add_to_album(self, album_name: str, photo: PhotoAsset) -> bool:
        """Add photo asset to an iCloud album."""
        try:
            albums = self.photos.albums
            if album_name not in albums:
                self.create_album(album_name)
                albums = self.photos.albums

            if album_name in albums:
                target_album = albums[album_name]
                return target_album.add_photo(photo)
        except Exception as e:
            logger.warning(f"Could not add photo to iCloud album '{album_name}': {e}")
        return False
