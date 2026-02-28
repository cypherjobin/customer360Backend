"""
VIBO Process Monitor
====================
Real-time monitoring of background processes.
Run this to check the status of all VIBO processes.
"""

import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

def check_chroma_progress():
    """Check ChromaDB embedding progress."""
    try:
        from vibo_vector_store import VectorStore
        s = VectorStore()
        s.initialize()
        stats = s.get_stats()

        transcripts_pct = (stats['transcripts_count'] / 2639) * 100
        events_pct = (stats['events_count'] / 3400) * 100

        return {
            'transcripts': f"{stats['transcripts_count']}/2639 ({transcripts_pct:.1f}%)",
            'events': f"{stats['events_count']}/3400 ({events_pct:.1f}%)",
            'location': stats['chroma_path'],
            'status': '🔄 Building' if stats['transcripts_count'] < 2639 else '✅ Complete'
        }
    except Exception as e:
        return {'status': f'❌ Error: {e}'}


def check_summariser_progress():
    """Check LLM summariser progress from log file."""
    log_file = Path("C:/Users/jjohn/.claude/projects/c--Projects-Customer360/tasks/bab6ede.output")

    if not log_file.exists():
        return {'status': '❌ Log file not found'}

    try:
        # Get last few lines to find current customer
        result = subprocess.run(
            ['powershell', '-Command', f'Get-Content "{log_file}" -Tail 20'],
            capture_output=True,
            text=True,
            timeout=5
        )

        lines = result.stdout.strip().split('\n')

        # Find the last customer number
        last_customer = None
        last_time = None

        for line in reversed(lines):
            if '[' in line and 'Customer:' in line:
                try:
                    parts = line.split('[')
                    if len(parts) > 1:
                        cust_part = parts[1].split(']')[0]
                        last_customer = cust_part.strip().split('Customer: ')[1]
                        break
                except:
                    continue

        # Check if process is still active (last line timestamp)
        if lines and 'INFO' in lines[-1]:
            time_str = lines[-1].split('[')[0].strip()
            try:
                # Parse timestamp like "2026-02-18 18:40:31,020"
                last_time_str = time_str.split('.')[0]
                last_time = datetime.strptime(last_time_str, '%Y-%m-%d %H:%M:%S')

                # Check if process is stale (>5 minutes old)
                if datetime.now() - last_time > timedelta(minutes=5):
                    status = '⏸️ Stopped (stale)'
                else:
                    status = '🔄 Running'
            except:
                status = '⏸️ Unknown status'

        return {
            'last_customer': last_customer,
            'total_customers': '~4,700',
            'progress': f"{int(1576/4700*100)}%" if last_customer else "Unknown",
            'last_activity': last_time_str if last_time else "Unknown",
            'status': status
        }

    except Exception as e:
        return {'status': f'❌ Error: {e}'}


def check_api_server():
    """Check if API server is running."""
    try:
        result = subprocess.run(
            ['curl', '-s', 'http://localhost:8000/health'],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0 and '"status":"healthy"' in result.stdout:
            return {'status': '✅ Running', 'url': 'http://localhost:8000'}
        else:
            return {'status': '❌ Down'}
    except:
        return {'status': '❌ Not responding'}


def display_dashboard():
    """Display monitoring dashboard."""
    print("\n" + "="*60)
    print(" " * 20 + "VIBO PROCESS MONITOR" + " " * 20)
    print("="*60)
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-"*60)

    # ChromaDB Progress
    print("\n📊 ChromaDB (Embedding Pipeline)")
    chroma = check_chroma_progress()
    for key, value in chroma.items():
        print(f"  {key.capitalize()}: {value}")

    # LLM Summariser
    print("\n🤖 LLM Summariser")
    summariser = check_summariser_progress()
    for key, value in summariser.items():
        print(f"  {key.capitalize()}: {value}")

    # API Server
    print("\n🌐 API Server")
    api = check_api_server()
    for key, value in api.items():
        print(f"  {key.capitalize()}: {value}")

    print("\n" + "="*60)

    # Estimated time remaining
    chroma = check_chroma_progress()
    if 'transcripts' in chroma:
        try:
            current = int(chroma['transcripts'].split('/')[0])
            remaining = 2639 - current
            if remaining > 0:
                est_minutes = remaining // 25  # ~25 per minute
                print(f"\n⏱️  Est. time remaining: ~{est_minutes} minutes for embeddings")
            else:
                print(f"\n✅ Embeddings complete! Ready for full semantic search")
        except:
            pass


if __name__ == "__main__":
    import sys

    # Fix Windows console encoding
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    # Display dashboard
    display_dashboard()

    # Optionally, refresh every 30 seconds
    if len(sys.argv) > 1 and sys.argv[1] == '--watch':
        print("\n🔄 Watching... (Ctrl+C to stop)")
        try:
            while True:
                time.sleep(30)
                print("\n" + "\n" * 3)
                display_dashboard()
        except KeyboardInterrupt:
            print("\n\n✋ Stopped monitoring")
