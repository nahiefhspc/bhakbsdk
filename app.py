import os
import re
import asyncio
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageEntityBold

# Load environment variables
load_dotenv()

# ============ CONFIGURATION FROM ENVIRONMENT VARIABLES ============
API_ID = int(os.getenv("API_ID", "23713783"))
API_HASH = os.getenv("API_HASH", "2daa157943cb2d76d149c4de0b036a99")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7213717609:AAGEyAJSfMUderWqlIAJkziRcIBrVTwjbXM")
PORT = int(os.getenv("PORT", 8080))  # 👈 Required for health check
# ========================================

user_data = {}


def get_user(user_id):
    if user_id not in user_data:
        user_data[user_id] = {
            'step': None, 'txt_lines': [], 'txt_filename': '', 'file_path': None,
            'channel_id': None, 'first_msg_id': None, 'last_msg_id': None,
            'fwd_step': None, 'fwd_target_channel': None, 'fwd_target_id_no_prefix': None,
            'fwd_txt_lines': [], 'fwd_txt_filename': '', 'fwd_file_path': None,
            'fwd_source_channel': None, 'fwd_first_msg_id': None, 'fwd_last_msg_id': None,
        }
    return user_data[user_id]


# ====================== HEALTH CHECK SERVER ======================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        pass  # Suppress logs

def start_http_server():
    """Run HTTP server for health checks in separate thread"""
    server = HTTPServer(('0.0.0.0', PORT), HealthHandler)
    print(f"🌐 Health check server running on port {PORT}")
    server.serve_forever()

# ====================== TELEGRAM BOT ======================

def parse_link(link):
    match = re.search(r't\.me/c/(\d+)/(\d+)', link)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def parse_target_channel(text):
    text = str(text).strip()
    if text.startswith('-100'):
        numeric = text[4:]
        return f"-100{numeric}", numeric
    elif text.startswith('-'):
        numeric = text[1:]
        return f"-100{numeric}", numeric
    else:
        if text.isdigit():
            return f"-100{text}", text
    return None, None


def extract_chapter_id_from_caption(caption):
    if not caption:
        return None
    match = re.search(r'ChapterId\s*>\s*(\S+)', caption)
    if match:
        return match.group(1).strip()
    return None


def remove_chapter_id_from_caption(caption):
    if not caption:
        return ""
    lines = caption.split('\n')
    new_lines = []
    for line in lines:
        if re.search(r'ChapterId\s*>', line):
            continue
        new_lines.append(line)
    result = '\n'.join(new_lines).strip()
    return result


def extract_keys_from_txt(txt_lines):
    results = []
    for idx, line in enumerate(txt_lines):
        line = line.strip()
        if not line:
            continue
        quality_matches = re.findall(r'(\d+)\(([^)]+)\)', line)
        if quality_matches:
            for quality, key in quality_matches:
                results.append({'line_idx': idx, 'quality': quality, 'key': key})
        else:
            match = re.search(r':\s+([A-Za-z0-9+/=_\-]{20,})\s*$', line)
            if match:
                key = match.group(1).strip()
                results.append({'line_idx': idx, 'quality': None, 'key': key})
    return results


async def process_forward(bot, event, user):
    source_full_id = user['fwd_source_channel']   
    source_disp_id = str(source_full_id).replace('-100', '') 
    target_full_id = user['fwd_target_channel']
    target_disp_id = user['fwd_target_id_no_prefix']
    
    first_msg_id = user['fwd_first_msg_id']
    last_msg_id = user['fwd_last_msg_id']
    txt_lines = user['fwd_txt_lines']
    original_filename = user['fwd_txt_filename']
    
    if first_msg_id > last_msg_id:
        first_msg_id, last_msg_id = last_msg_id, first_msg_id

    total_messages = last_msg_id - first_msg_id + 1
    status_msg = await event.respond(f"🔄 **Forward Process Started**\n📊 Scanning: ~{total_messages} messages\n⏳ Please wait...")

    key_to_message = {}
    processed_count = 0
    last_edit_time = 0

    try:
        chunk_size = 200
        msg_ids = list(range(first_msg_id, last_msg_id + 1))

        for i in range(0, len(msg_ids), chunk_size):
            chunk = msg_ids[i:i + chunk_size]
            retry = 0
            
            while retry < 3:
                try:
                    messages = await bot.get_messages(source_full_id, ids=chunk)
                    
                    for msg in messages:
                        if msg is None: continue
                        caption = getattr(msg, 'raw_text', '') or getattr(msg, 'text', '') or ''
                        if caption:
                            key = extract_chapter_id_from_caption(caption)
                            if key:
                                if key not in key_to_message:
                                    key_to_message[key] = msg
                    processed_count += len(chunk)
                    break
                except FloodWaitError as fw:
                    await asyncio.sleep(fw.seconds + 1)
                    retry += 1
                except Exception as err:
                    print(f"Scan Error: {err}")
                    processed_count += len(chunk)
                    break
            
            now = time.time()
            if now - last_edit_time > 5:
                pct = int((processed_count / max(total_messages, 1)) * 100)
                await status_msg.edit(f"🔄 **Scanning...** ({pct}%)\n🔑 Found Keys: {len(key_to_message)}")
                last_edit_time = now
                
        await status_msg.edit(f"✅ **Scan Complete!**\n🔑 Found {len(key_to_message)} unique keys.\n⚙️ Processing .TXT keys...")
        await asyncio.sleep(1)

        txt_entries = extract_keys_from_txt(txt_lines)
        line_mappings = {} 
        
        await status_msg.edit(f"🚀 Starting Forward...")
        
        forwarded_count = 0
        not_found_count = 0
        mapping_log = [] 
        
        last_edit_time = time.time()
        base_name = original_filename.rsplit('.', 1)[0] if '.' in original_filename else original_filename

        for entry in txt_entries:
            line_idx = entry['line_idx']
            quality = entry['quality']
            key = entry['key']
            
            source_msg = key_to_message.get(key)
            
            if not source_msg:
                not_found_count += 1
                if line_idx not in line_mappings: line_mappings[line_idx] = []
                line_mappings[line_idx].append({'qual': quality, 'key': key, 'url': None})
                continue
                
            src_cap = source_msg.raw_text or source_msg.text or ""
            new_cap = remove_chapter_id_from_caption(src_cap)
            
            bold_entities = [MessageEntityBold(0, len(new_cap))] if new_cap else []
            
            retry_fwd = 0
            fwd_msg = None
            while retry_fwd < 5:
                try:
                    if source_msg.media:
                        fwd_msg = await bot.send_file(target_full_id, source_msg.media, caption=new_cap, entities=bold_entities, force_document=True)
                    else:
                        fwd_msg = await bot.send_message(target_full_id, new_cap, entities=bold_entities)
                    break
                except FloodWaitError as fw:
                    await asyncio.sleep(fw.seconds + 2)
                    retry_fwd += 1
                except Exception as f_err:
                    print(f"Fwd Error {key}: {f_err}")
                    retry_fwd += 1
                    await asyncio.sleep(3)
            
            if fwd_msg:
                forwarded_count += 1
                target_link = f"https://t.me/c/{target_disp_id}/{fwd_msg.id}"
                source_link = f"https://t.me/c/{source_disp_id}/{source_msg.id}"
                
                mapping_log.append(f"{source_link} >> {target_link}")
                
                if line_idx not in line_mappings: line_mappings[line_idx] = []
                line_mappings[line_idx].append({'qual': quality, 'key': key, 'url': target_link})
            else:
                not_found_count += 1
                if line_idx not in line_mappings: line_mappings[line_idx] = []
                line_mappings[line_idx].append({'qual': quality, 'key': key, 'url': None})

            now = time.time()
            if now - last_edit_time > 5:
                await status_msg.edit(f"📤 **Forwarding...**\n✅ Sent: {forwarded_count} | ❌ Missing: {not_found_count}")
                last_edit_time = now

        await status_msg.edit("📁 **Generating Result Files...**")
        await asyncio.sleep(1)

        final_txt_content = list(txt_lines)
        for idx, mappings in line_mappings.items():
            if idx < len(final_txt_content):
                for m in sorted(mappings, key=lambda x: (x['qual'] or ''), reverse=True):
                    if m['url']:
                        old_str = f"{m['qual']}({m['key']})" if m['qual'] else m['key']
                        final_txt_content[idx] = final_txt_content[idx].replace(old_str, m['url'])

        tmp_base = f"./downloads/{event.sender_id}_{int(time.time())}"
        path_txt = f"{tmp_base}_updated.txt"
        path_map = f"{tmp_base}_mapping.txt"
        
        with open(path_txt, 'w', encoding='utf-8') as f:
            f.write('\n'.join(final_txt_content))
        
        mapping_text = '\n'.join(mapping_log) if mapping_log else "No mappings found."
        with open(path_map, 'w', encoding='utf-8') as f:
            f.write(f"# Forward Mapping Report\n# Total Sent: {forwarded_count}\n\n{mapping_text}")

        await bot.send_file(target_full_id, path_txt, caption=f"📄 **Updated List** (`{base_name}_updated.txt`)", file_name=f"{base_name}_updated.txt", force_document=True)
        await asyncio.sleep(2)
        await bot.send_file(target_full_id, path_map, caption=f"🗺️ **Mapping Report**", file_name=f"{base_name}_mapping.txt", force_document=True)

        cleanup_file(path_txt); cleanup_file(path_map); cleanup_file(user['fwd_file_path'])
        await status_msg.edit(f"🎉 **Task Completed!**\n✅ Forwarded: **{forwarded_count}**")
        user['fwd_step'] = None; user['fwd_file_path'] = None

    except Exception as e:
        await event.respond(f"❌ **Error:**\n```{str(e)}```")
        user['fwd_step'] = None
        cleanup_all_files(user)
        import traceback; traceback.print_exc()


def cleanup_file(file_path):
    if file_path and os.path.exists(file_path):
        try: os.remove(file_path); print(f"Deleted: {file_path}")
        except: pass

def cleanup_all_files(user):
    cleanup_file(user.get('file_path'))
    cleanup_file(user.get('fwd_file_path'))


async def process_filter(bot, event, user):
    pass


async def main():
    # Start HTTP Health Check Server in background thread
    http_thread = threading.Thread(target=start_http_server, daemon=True)
    http_thread.start()
    
    bot = TelegramClient('filter_bot', API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)
    os.makedirs('./downloads', exist_ok=True)
    print("🚀 Bot Online! | 🌐 Health Check: http://localhost:{PORT}/health")

    @bot.on(events.NewMessage(pattern='/start'))
    async def start_h(event): await event.respond("🤖 **Bot Active**\nCommands:\n/filter\n/forward\n/cancel")
    @bot.on(events.NewMessage(pattern='/cancel'))
    async def cancel_h(event): cleanup_all_files(get_user(event.sender_id)); await event.respond("❌ Cancelled."); get_user(event.sender_id)['step']=None

    @bot.on(events.NewMessage(pattern='/filter'))
    async def filter_cmd(event):
        u = get_user(event.sender_id)
        u['step'] = 'wait_txt'; u['fwd_step'] = None
        u['txt_lines'], u['txt_filename'], u['file_path'] = [], "", None
        u['channel_id'], u['first_msg_id'], u['last_msg_id'] = None, None, None
        await event.respond("📄 **Send .txt file**")

    @bot.on(events.NewMessage(pattern='/forward'))
    async def forward_cmd(event):
        u = get_user(event.sender_id)
        u['fwd_step'] = 'wait_target'; u['step'] = None
        u['fwd_txt_lines'], u['fwd_txt_filename'], u['fwd_file_path'] = [], "", None
        u['fwd_target_channel'], u['fwd_target_id_no_prefix'] = None, None
        u['fwd_source_channel'], u['fwd_first_msg_id'], u['fwd_last_msg_id'] = None, None, None
        await event.respond("1️⃣ **Send Target Channel ID**")

    @bot.on(events.NewMessage)
    async def handler(event):
        if event.text and event.text.startswith('/'): return
        u = get_user(event.sender_id)

        # FILTER FLOW
        if u['step'] == 'wait_txt':
            if event.document and event.document.file_name.endswith('.txt'):
                pth = await event.download_media(file=f'./downloads/{event.sender_id}_{event.document.file_name}')
                with open(pth, 'r', errors='ignore') as f: lines = [l.strip() for l in f.readlines()]
                u['txt_lines'], u['txt_filename'], u['file_path'] = lines, event.document.file_name, pth
                u['step'] = 'wait_flink'; await event.respond(f"✅ File Received\n🔗 Send First Link: `https://t.me/c/id/msg`")
            return
        if u['step'] == 'wait_flink':
            cid, mid = parse_link(event.text or '')
            if cid: u['channel_id'], u['first_msg_id'] = cid, mid; u['step'] = 'wait_llink'; await event.respond(f"✅ Set First `{mid}`\n🔗 Send Last Link:")
            else: await event.respond("Invalid Link.")
            return
        if u['step'] == 'wait_llink':
            cid, mid = parse_link(event.text or '')
            if cid and cid == u['channel_id']:
                u['last_msg_id'] = mid; u['step'] = 'run'; await event.respond("🚀 Processing..."); await process_filter(bot, event, u)
            else: await event.respond("Mismatched Source.")
            return

        # FORWARD FLOW
        if u['fwd_step'] == 'wait_target':
            full, disp = parse_target_channel(event.text)
            if full: u['fwd_target_channel'], u['fwd_target_id_no_prefix'] = full, disp; u['fwd_step'] = 'wait_fwd_txt'; await event.respond(f"✅ Target Set `{disp}`\n📄 Send TXT File:")
            else: await event.respond("Invalid ID.")
            return
        if u['fwd_step'] == 'wait_fwd_txt':
            if event.document and event.document.file_name.endswith('.txt'):
                pth = await event.download_media(file=f'./downloads/{event.sender_id}_fwd_{event.document.file_name}')
                with open(pth, 'r', errors='ignore') as f: lines = [l.strip() for l in f.readlines()]
                u['fwd_txt_lines'], u['fwd_txt_filename'], u['fwd_file_path'] = lines, event.document.file_name, pth
                u['fwd_step'] = 'wait_src_flink'; await event.respond(f"✅ TXT Received\n🔗 Send Source First Link: `https://t.me/c/id/msg`")
            else: await event.respond("❌ Send .txt")
            return
        if u['fwd_step'] == 'wait_src_flink':
            cid, mid = parse_link(event.text or '')
            if cid: u['fwd_source_channel'], u['fwd_first_msg_id'] = f"-100{cid}", mid; u['fwd_step'] = 'wait_src_llink'; await event.respond(f"✅ Source First `{mid}`\n🔗 Send Source Last Link:")
            else: await event.respond("Invalid Link.")
            return
        if u['fwd_step'] == 'wait_src_llink':
            cid, mid = parse_link(event.text or '')
            if cid and f"-100{cid}" == u['fwd_source_channel']:
                u['fwd_last_msg_id'] = mid; u['fwd_step'] = 'run'; await event.respond("🚀 Starting Forward..."); await process_forward(bot, event, u)
            else: await event.respond("Source Mismatch!")
            return

    print("✅ Running...")
    await bot.run_until_disconnected()

if __name__ == '__main__':
    loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: print("\n🛑 Stopped!");
    finally: loop.close()
