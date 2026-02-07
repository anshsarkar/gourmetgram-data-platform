#!/usr/bin/env python3
"""
Test script to verify milestone persistence to PostgreSQL
"""
import sys
from pathlib import Path
import psycopg2
from tabulate import tabulate

# Add stream_consumer directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'stream_consumer'))

from config import config


def test_milestone_persistence():
    print("=" * 70)
    print("MILESTONE PERSISTENCE TEST")
    print("=" * 70)
    print()

    try:
        # Connect to database
        print(f"Connecting to database...")
        conn = psycopg2.connect(config.database_url)
        cursor = conn.cursor()

        # Check if image_milestones table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'image_milestones'
            )
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            print("[ERROR] image_milestones table does not exist!")
            print("        Make sure to restart the API service to create the table.")
            return False

        print("[OK] image_milestones table exists")
        print()

        # Count total milestones
        cursor.execute("SELECT COUNT(*) FROM image_milestones")
        total_count = cursor.fetchone()[0]

        print(f"Total milestones persisted: {total_count}")
        print()

        if total_count == 0:
            print("[INFO] No milestones persisted yet.")
            print("       Wait for images to reach 100, 1000, or 10000 views.")
            print("       Check stream consumer logs for milestone events.")
            return True

        # Get milestone distribution
        cursor.execute("""
            SELECT milestone_value, COUNT(*) as count
            FROM image_milestones
            WHERE milestone_type = 'views'
            GROUP BY milestone_value
            ORDER BY milestone_value
        """)

        milestone_distribution = cursor.fetchall()

        print("Milestone Distribution:")
        print(tabulate(
            milestone_distribution,
            headers=["Milestone Value", "Count"],
            tablefmt="simple"
        ))
        print()

        # Get recent milestones (last 10)
        cursor.execute("""
            SELECT
                SUBSTRING(image_id::text, 1, 16) as image_id,
                milestone_type,
                milestone_value,
                reached_at
            FROM image_milestones
            ORDER BY reached_at DESC
            LIMIT 10
        """)

        recent_milestones = cursor.fetchall()

        print("Recent Milestones (last 10):")
        print(tabulate(
            recent_milestones,
            headers=["Image ID", "Type", "Value", "Reached At"],
            tablefmt="simple"
        ))
        print()

        # Check for duplicate milestones (should be 0)
        cursor.execute("""
            SELECT image_id, milestone_type, milestone_value, COUNT(*) as dup_count
            FROM image_milestones
            GROUP BY image_id, milestone_type, milestone_value
            HAVING COUNT(*) > 1
        """)

        duplicates = cursor.fetchall()

        if duplicates:
            print("[WARNING] Found duplicate milestones:")
            print(tabulate(
                duplicates,
                headers=["Image ID", "Type", "Value", "Duplicate Count"],
                tablefmt="simple"
            ))
            print()
        else:
            print("[OK] No duplicate milestones found")
            print()

        cursor.close()
        conn.close()

        print("=" * 70)
        print("[OK] Milestone persistence test complete")
        print("=" * 70)

        return True

    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        return False


if __name__ == "__main__":
    success = test_milestone_persistence()
    exit(0 if success else 1)
