#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_feedback_review_queue import PrimaryResultFeedbackReviewQueue


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage the governed primary result feedback review queue.")
    parser.add_argument("--queue-dir", default="artifacts/primary_result_feedback_review_queue")
    parser.add_argument("--feedback-json", help="Learning feedback artifact to enqueue.")
    parser.add_argument("--review-id")
    parser.add_argument("--owner", default="unassigned")
    parser.add_argument("--decision-status", choices=["accepted", "rejected", "needs_benchmark", "closed"])
    parser.add_argument("--reason")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    queue = PrimaryResultFeedbackReviewQueue(queue_dir=args.queue_dir)
    try:
        if args.list:
            payload = {
                "status": "listed",
                "items": queue.list_items(),
                "summary_path": str(queue.summary_path),
                "decision_history_path": str(queue.decisions_path),
            }
        elif args.feedback_json:
            item = queue.enqueue(feedback_path=args.feedback_json, review_id=args.review_id, owner=args.owner)
            payload = {
                "status": "enqueued",
                "review_id": item["review_id"],
                "item_status": item["status"],
                "requires_baseline_revalidation": item["requires_baseline_revalidation"],
                "item_path": str(queue.items_dir / f"{item['review_id']}.json"),
                "summary_path": str(queue.summary_path),
                "decision_history_path": str(queue.decisions_path),
            }
        elif args.decision_status:
            if not args.review_id:
                raise ValueError("--review-id is required for decisions")
            if not args.reason:
                raise ValueError("--reason is required for decisions")
            item = queue.decide(
                review_id=args.review_id,
                status=args.decision_status,
                reason=args.reason,
                actor=args.owner,
            )
            payload = {
                "status": "decided",
                "review_id": item["review_id"],
                "item_status": item["status"],
                "requires_baseline_revalidation": item["requires_baseline_revalidation"],
                "do_not_auto_apply": item["do_not_auto_apply"],
                "summary_path": str(queue.summary_path),
                "decision_history_path": str(queue.decisions_path),
            }
        else:
            raise ValueError("provide --feedback-json, --decision-status, or --list")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
