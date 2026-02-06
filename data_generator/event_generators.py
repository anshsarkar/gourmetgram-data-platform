import httpx
import random
import logging
import string
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


# Food-11 dataset class mapping (class_00 to class_10)
FOOD11_CATEGORIES = [
    "Bread",           # class_00
    "Dairy product",   # class_01
    "Dessert",         # class_02
    "Egg",             # class_03
    "Fried food",      # class_04
    "Meat",            # class_05
    "Noodles/Pasta",   # class_06
    "Rice",            # class_07
    "Seafood",         # class_08
    "Soup",            # class_09
    "Vegetable/Fruit"  # class_10
]


def random_string(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_letters, k=length))


class GourmetGramEventGenerators:

    def __init__(self, api_base_url: str, dataset_path: Path, timeout: float = 30.0):
        self.api_url = api_base_url
        self.dataset_path = Path(dataset_path)
        self.client = httpx.AsyncClient(timeout=timeout)

        # State management - track created entities for causality
        self.users: List[str] = []
        self.images: List[str] = []
        self.comments: List[str] = []

        # Counters for statistics
        self.stats = {
            "users": 0,
            "uploads": 0,
            "views": 0,
            "comments": 0,
            "flags": 0,
            "errors": 0
        }

        logger.info(f"Initialized event generators (API: {api_base_url})")

        if not self.dataset_path.exists():
            logger.warning(f"Dataset path does not exist: {self.dataset_path}")
        else:
            logger.info(f"Dataset path: {self.dataset_path}")

    async def generate_user(self) -> Optional[Dict]:
        try:
            username = random_string(12)

            response = await self.client.post(
                f"{self.api_url}/users/",
                json={"username": username}
            )
            response.raise_for_status()

            user_data = response.json()
            user_id = user_data["id"]

            self.users.append(user_id)
            self.stats["users"] += 1

            logger.info(f"Created user: {username} ({user_id})")
            return user_data

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to create user: {e}")
            return None

    async def generate_upload(self) -> Optional[Dict]:
        if not self.users:
            logger.warning("No users available - creating one first")
            await self.generate_user()
            if not self.users:
                logger.error("Failed to create user - cannot upload image")
                return None

        try:
            image_path = self._get_random_image()
            if not image_path:
                logger.error("No images found in dataset")
                return None

            category = self._extract_category(image_path)
            caption = random_string(20)
            user_id = random.choice(self.users)

            with open(image_path, 'rb') as f:
                files = {'file': (image_path.name, f, 'image/jpeg')}
                data = {
                    'user_id': user_id,
                    'caption': caption,
                    'category': category
                }

                response = await self.client.post(
                    f"{self.api_url}/upload/",
                    files=files,
                    data=data
                )
                response.raise_for_status()

            image_data = response.json()
            image_id = image_data["id"]

            self.images.append(image_id)
            self.stats["uploads"] += 1

            logger.info(
                f"Uploaded image: {category} by user {user_id[:8]}... "
                f"(id: {image_id[:8]}...)"
            )
            return image_data

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to upload image: {e}")
            return None

    async def generate_view(self) -> bool:
        if not self.images:
            logger.debug("No images available to view - skipping")
            return False

        try:
            image_id = random.choice(self.images)

            response = await self.client.post(
                f"{self.api_url}/images/{image_id}/view"
            )
            response.raise_for_status()

            result = response.json()
            view_count = result.get("views", "?")

            self.stats["views"] += 1
            logger.debug(f"Recorded view on image {image_id[:8]}... (total: {view_count})")
            return True

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to record view: {e}")
            return False

    async def generate_comment(self) -> Optional[Dict]:
        if not self.images:
            logger.debug("No images available to comment on - skipping")
            return None

        if not self.users:
            logger.warning("No users available - creating one first")
            await self.generate_user()
            if not self.users:
                return None

        try:
            image_id = random.choice(self.images)
            user_id = random.choice(self.users)
            content = random_string(15)

            response = await self.client.post(
                f"{self.api_url}/comments/",
                json={
                    "image_id": image_id,
                    "user_id": user_id,
                    "content": content
                }
            )
            response.raise_for_status()

            comment_data = response.json()
            comment_id = comment_data["id"]

            self.comments.append(comment_id)
            self.stats["comments"] += 1

            logger.info(
                f"Added comment on image {image_id[:8]}... "
                f"by user {user_id[:8]}... (id: {comment_id[:8]}...)"
            )
            return comment_data

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to add comment: {e}")
            return None

    async def generate_flag(self) -> Optional[Dict]:
        if not self.users:
            logger.warning("No users available - creating one first")
            await self.generate_user()
            if not self.users:
                return None

        can_flag_image = len(self.images) > 0
        can_flag_comment = len(self.comments) > 0

        if not can_flag_image and not can_flag_comment:
            logger.debug("No images or comments to flag - skipping")
            return None

        try:
            if can_flag_image and can_flag_comment:
                flag_target = random.choices(['image', 'comment'], weights=[0.7, 0.3])[0]
            elif can_flag_image:
                flag_target = 'image'
            else:
                flag_target = 'comment'

            user_id = random.choice(self.users)

            payload = {
                "user_id": user_id,
                "reason": random_string(15)
            }

            if flag_target == 'image':
                target_id = random.choice(self.images)
                payload["image_id"] = target_id
                target_type = "image"
            else:
                target_id = random.choice(self.comments)
                payload["comment_id"] = target_id
                target_type = "comment"

            response = await self.client.post(
                f"{self.api_url}/flags/",
                json=payload
            )
            response.raise_for_status()

            flag_data = response.json()
            flag_id = flag_data["id"]

            self.stats["flags"] += 1

            logger.info(
                f"Created flag for {target_type} {target_id[:8]}... "
                f"by user {user_id[:8]}... (reason: {payload['reason']})"
            )
            return flag_data

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to create flag: {e}")
            return None

    def _get_random_image(self) -> Optional[Path]:
        try:
            splits = ['training', 'validation', 'evaluation']
            available_splits = [
                s for s in splits
                if (self.dataset_path / s).exists()
            ]

            if not available_splits:
                logger.error(f"No splits found in {self.dataset_path}")
                return None

            split = random.choice(available_splits)
            split_path = self.dataset_path / split

            image_extensions = {'.jpg', '.jpeg', '.png', '.JPG', '.JPEG', '.PNG'}
            images = [
                f for f in split_path.rglob('*')
                if f.is_file() and f.suffix in image_extensions
            ]

            if not images:
                logger.error(f"No images found in {split_path}")
                return None

            return random.choice(images)

        except Exception as e:
            logger.error(f"Error selecting random image: {e}")
            return None

    def _extract_category(self, image_path: Path) -> str:
        try:
            class_dir = image_path.parent.name

            if class_dir.startswith("class_"):
                class_idx = int(class_dir.split("_")[1])

                if 0 <= class_idx < len(FOOD11_CATEGORIES):
                    return FOOD11_CATEGORIES[class_idx]

            logger.warning(f"Could not extract category from {image_path}")
            return "Unknown"

        except Exception as e:
            logger.error(f"Error extracting category: {e}")
            return "Unknown"

    def print_stats(self):
        logger.info("=" * 50)
        logger.info("EVENT GENERATOR STATISTICS")
        logger.info("=" * 50)
        logger.info(f"Users created:     {self.stats['users']}")
        logger.info(f"Images uploaded:   {self.stats['uploads']}")
        logger.info(f"Views recorded:    {self.stats['views']}")
        logger.info(f"Comments added:    {self.stats['comments']}")
        logger.info(f"Flags created:     {self.stats['flags']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 50)
        logger.info(f"Current state: {len(self.users)} users, "
                   f"{len(self.images)} images, {len(self.comments)} comments")
        logger.info("=" * 50)

    async def close(self):
        await self.client.aclose()
        logger.info("Closed HTTP client")
