import os
import re
import asyncio
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageEntityBold, DocumentAttributeFilename

# Load environment variables
load_dotenv()

# ============ CONFIGURATION ============
API_ID = int(os.getenv("API_ID", "23713783"))
API_HASH = os.getenv("API_HASH", "2daa157943cb2d76d149c4de0b036a99")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7213717609:AAGEyAJSfMUderWqlIAJkziRcIBrVTwjbXM")
PORT = int(os.getenv("PORT", 8080))
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


# ✅ SAFE FILE NAME EXTRACTION
def get_file_name(document):
    """Safely extract file_name from Document attributes"""
    if not document:
        return ""
    if hasattr(document, 'attributes'):
        for attr in document.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name
    return ""


# ====================== HEALTH CHECK SERVER ======================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')

    def log_message(self, format, *args):
        pass

def start_http_server():
    server = HTTPServer(('0.0.0.0', PORT), HealthHandler)
    print(f"🌐 Health check running on port {PORT}")
    server.serve_forever()


# ====================== HELPER FUNCTIONS ======================
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
    return '\n'.join(new_lines).strip()


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


def is_failed_message(text):
    if not text:
        return False
    return 'Downloading Failed' in text or 'DRM MPD Downloading Failed' in text


def is_uploading_message(text):
    if not text:
        return False
    return 'Uploading Video:' in text


def extract_failed_number(text):
    match = re.search(r'Name\s*=\s*>+\s*(\d+)', text)
    if match:
        return int(match.group(1))
    return None


def extract_failed_video_name(text):
    match = re.search(r'Name\s*=\s*>+\s*\d+\s+(.+?)(?:\n|$)', text)
    if match:
        return match.group(1).strip()
    return None


def extract_uploading_video_name(text):
    lines = text.split('\n')
    for i, line in enumerate(lines):
        if 'Uploading Video:' in line:
            remaining = '\n'.join(lines[i + 1:]).strip()
            if remaining:
                return remaining.split('\n')[0].strip()
    return None


def cleanup_file(file_path):
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            print(f"🗑️ Deleted: {file_path}")
        except:
            pass


def cleanup_all_files(user):
    cleanup_file(user.get('file_path'))
    cleanup_file(user.get('fwd_file_path'))


def get_output_filename(original_filename):
    if original_filename.lower().endswith('.txt'):
        base = original_filename[:-4]
        return f"{base}_updated.txt"
    return f"{original_filename}_updated.txt"


# ====================== FORWARD PROCESS ======================
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
    status_msg = await event.respond(
        f"🔄 **Forward Process Started**\n"
        f"📊 Scanning: ~{total_messages} messages\n"
        f"⏳ Please wait..."
    )

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
                        if msg is None:
                            continue
                        caption = getattr(msg, 'raw_text', '') or getattr(msg, 'text', '') or ''
                        if caption:
                            key = extract_chapter_id_from_caption(caption)
                            if key and key not in key_to_message:
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
                try:
                    await status_msg.edit(
                        f"🔄 **Scanning...** ({pct}%)\n🔑 Found Keys: {len(key_to_message)}"
                    )
                except:
                    pass
                last_edit_time = now

        try:
            await status_msg.edit(
                f"✅ **Scan Complete!**\n"
                f"🔑 Found {len(key_to_message)} unique keys.\n"
                f"⚙️ Processing .TXT keys..."
            )
        except:
            pass
        await asyncio.sleep(1)

        txt_entries = extract_keys_from_txt(txt_lines)
        line_mappings = {}

        try:
            await status_msg.edit(f"🚀 Starting Forward...")
        except:
            pass

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
                if line_idx not in line_mappings:
                    line_mappings[line_idx] = []
                line_mappings[line_idx].append({'qual': quality, 'key': key, 'url': None})
                continue

            src_cap = source_msg.raw_text or source_msg.text or ""
            new_cap = remove_chapter_id_from_caption(src_cap)

            # ✅ BOLD CAPTION
            bold_entities = [MessageEntityBold(0, len(new_cap))] if new_cap else []

            retry_fwd = 0
            fwd_msg = None
            while retry_fwd < 5:
                try:
                    if source_msg.media:
                        fwd_msg = await bot.send_file(
                            target_full_id,
                            source_msg.media,
                            caption=new_cap,
                            entities=bold_entities,
                            force_document=True
                        )
                    else:
                        fwd_msg = await bot.send_message(
                            target_full_id,
                            new_cap,
                            entities=bold_entities
                        )
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

                if line_idx not in line_mappings:
                    line_mappings[line_idx] = []
                line_mappings[line_idx].append({'qual': quality, 'key': key, 'url': target_link})
            else:
                not_found_count += 1
                if line_idx not in line_mappings:
                    line_mappings[line_idx] = []
                line_mappings[line_idx].append({'qual': quality, 'key': key, 'url': None})

            now = time.time()
            if now - last_edit_time > 5:
                try:
                    await status_msg.edit(
                        f"📤 **Forwarding...**\n"
                        f"✅ Sent: {forwarded_count} | ❌ Missing: {not_found_count}"
                    )
                except:
                    pass
                last_edit_time = now

        try:
            await status_msg.edit("📁 **Generating Result Files...**")
        except:
            pass
        await asyncio.sleep(1)

        # Build updated txt
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

        await bot.send_file(
            target_full_id, path_txt,
            caption=f"📄 **Updated List** (`{base_name}_updated.txt`)",
            file_name=f"{base_name}_updated.txt",
            force_document=True
        )
        await asyncio.sleep(2)
        await bot.send_file(
            target_full_id, path_map,
            caption=f"🗺️ **Mapping Report**",
            file_name=f"{base_name}_mapping.txt",
            force_document=True
        )

        cleanup_file(path_txt)
        cleanup_file(path_map)
        cleanup_file(user['fwd_file_path'])

        try:
            await status_msg.edit(
                f"🎉 **Task Completed!**\n"
                f"✅ Forwarded: **{forwarded_count}**\n"
                f"❌ Not Found: **{not_found_count}**"
            )
        except:
            pass

        user['fwd_step'] = None
        user['fwd_file_path'] = None

    except Exception as e:
        await event.respond(f"❌ **Error:**\n```{str(e)}```")
        user['fwd_step'] = None
        cleanup_all_files(user)
        import traceback
        traceback.print_exc()


# ====================== FILTER PROCESS ======================
async def process_filter(bot, event, user):
    channel_id = user['channel_id']
    first_msg_id = user['first_msg_id']
    last_msg_id = user['last_msg_id']
    txt_lines = user['txt_lines']
    original_filename = user['txt_filename']
    downloaded_file_path = user['file_path']

    if first_msg_id > last_msg_id:
        first_msg_id, last_msg_id = last_msg_id, first_msg_id

    full_channel_id = int(f"-100{channel_id}")
    total_messages = last_msg_id - first_msg_id + 1

    failed_numbers = []
    failed_names = []
    uploading_msgs = []
    processed = 0
    failed_count = 0
    deleted_count = 0

    status_msg = await event.respond(
        f"🔄 **Processing...**\n"
        f"📊 Total: ~{total_messages} messages\n"
        f"📄 TXT Lines: {len(txt_lines)}\n"
        f"📁 File: `{original_filename}`\n"
        f"⏳ Please wait..."
    )

    try:
        msg_ids = list(range(first_msg_id, last_msg_id + 1))
        chunk_size = 200
        last_edit_time = 0

        for i in range(0, len(msg_ids), chunk_size):
            chunk = msg_ids[i:i + chunk_size]
            retry_count = 0

            while retry_count < 3:
                try:
                    messages = await bot.get_messages(full_channel_id, ids=chunk)

                    for msg in messages:
                        if msg is None:
                            processed += 1
                            continue

                        text = ''
                        if hasattr(msg, 'raw_text') and msg.raw_text:
                            text = msg.raw_text
                        elif hasattr(msg, 'text') and msg.text:
                            text = msg.text
                        elif hasattr(msg, 'message') and msg.message:
                            text = msg.message

                        if not text:
                            processed += 1
                            continue

                        if is_failed_message(text):
                            num = extract_failed_number(text)
                            name = extract_failed_video_name(text)

                            if num is not None:
                                failed_numbers.append(num)
                            if name:
                                failed_names.append(name)

                            failed_count += 1

                        elif is_uploading_message(text):
                            uname = extract_uploading_video_name(text)
                            if uname:
                                uploading_msgs.append({
                                    'msg_id': msg.id,
                                    'name': uname
                                })

                        processed += 1

                    break

                except FloodWaitError as e:
                    try:
                        await status_msg.edit(
                            f"⚠️ **FloodWait {e.seconds}s...**\n"
                            f"Processed: {processed}/{total_messages}"
                        )
                    except:
                        pass
                    await asyncio.sleep(e.seconds + 2)
                    retry_count += 1

                except Exception as e:
                    print(f"Chunk error: {e}")
                    processed += len(chunk)
                    break

            now = time.time()
            if now - last_edit_time > 4:
                try:
                    pct = int((processed / total_messages) * 100)
                    await status_msg.edit(
                        f"🔄 **Processing... {pct}%**\n"
                        f"📊 {processed}/{total_messages}\n"
                        f"❌ Failed: {failed_count}\n"
                        f"⏳ Wait..."
                    )
                    last_edit_time = now
                except:
                    pass

        # DELETE Uploading Video messages
        try:
            await status_msg.edit(
                f"🗑️ **Deleting Uploading Video msgs...**\n"
                f"Checking {len(uploading_msgs)} msgs..."
            )
        except:
            pass

        msgs_to_delete = []
        for upload in uploading_msgs:
            upload_clean = re.sub(r'\s+', ' ', upload['name']).strip().lower()

            for fname in failed_names:
                fname_clean = re.sub(r'\s+', ' ', fname).strip().lower()

                if fname_clean in upload_clean or upload_clean in fname_clean:
                    msgs_to_delete.append(upload['msg_id'])
                    break

        if msgs_to_delete:
            for i in range(0, len(msgs_to_delete), 100):
                batch = msgs_to_delete[i:i + 100]
                retry = 0
                while retry < 3:
                    try:
                        await bot.delete_messages(full_channel_id, batch)
                        deleted_count += len(batch)
                        break
                    except FloodWaitError as e:
                        await asyncio.sleep(e.seconds + 1)
                        retry += 1
                    except Exception as e:
                        print(f"Delete error: {e}")
                        break

        # MATCH LINE NUMBERS FROM .txt
        failed_lines = []
        invalid_numbers = []

        for num in failed_numbers:
            index = num - 1
            if 0 <= index < len(txt_lines):
                failed_lines.append(txt_lines[index])
            else:
                invalid_numbers.append(num)

        seen = set()
        unique_failed_lines = []
        for line in failed_lines:
            if line not in seen:
                unique_failed_lines.append(line)
                seen.add(line)
        failed_lines = unique_failed_lines

        # CREATE OUTPUT FILE
        output_display_name = get_output_filename(original_filename)
        output_temp_path = f"./downloads/{event.sender_id}_{output_display_name}"

        with open(output_temp_path, 'w', encoding='utf-8') as f:
            if failed_lines:
                for line in failed_lines:
                    f.write(line + '\n')
            else:
                f.write("No failed lines found.\n")

        nums_str = ', '.join(str(n) for n in failed_numbers[:80])
        if len(failed_numbers) > 80:
            nums_str += '...'

        try:
            await status_msg.edit(
                f"✅ **Processing Complete!**\n\n"
                f"📊 **Stats:**\n"
                f"├ Scanned: **{processed}**\n"
                f"├ Failed Msgs: **{failed_count}**\n"
                f"├ Line Numbers: **{len(failed_numbers)}**\n"
                f"├ TXT Lines Matched: **{len(failed_lines)}**\n"
                f"├ Uploading Deleted: **{deleted_count}**\n"
                f"└ 📄 Uploading `{output_display_name}`..."
            )
        except:
            pass

        caption = (
            f"📄 **{output_display_name}**\n\n"
            f"❌ Failed: **{failed_count}**\n"
            f"📝 Lines extracted: **{len(failed_lines)}**\n"
            f"🗑️ Deleted: **{deleted_count}**\n"
        )

        if invalid_numbers:
            caption += f"\n⚠️ Out of range: {invalid_numbers[:20]}"

        if nums_str:
            caption += f"\n\n🔢 Numbers: `{nums_str}`"

        await bot.send_file(
            event.chat_id,
            output_temp_path,
            caption=caption,
            file_name=output_display_name,
            force_document=True
        )

        cleanup_file(output_temp_path)
        cleanup_file(downloaded_file_path)
        user['file_path'] = None
        user['step'] = None

    except Exception as e:
        await event.respond(
            f"❌ **Error!**\n```{str(e)}```\n"
            f"Try /filter again"
        )
        user['step'] = None
        cleanup_file(downloaded_file_path)
        user['file_path'] = None
        import traceback
        traceback.print_exc()


# ====================== MAIN BOT ======================
async def main():
    # Start HTTP Health Check Server
    http_thread = threading.Thread(target=start_http_server, daemon=True)
    http_thread.start()

    bot = TelegramClient('filter_bot', API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)
    os.makedirs('./downloads', exist_ok=True)
    print(f"🚀 Bot Online! | 🌐 Health: http://0.0.0.0:{PORT}/health")

    @bot.on(events.NewMessage(pattern='/start'))
    async def start_h(event):
        await event.respond(
            "🤖 **Bot Active**\n\n"
            "📌 **Commands:**\n"
            "/filter - Scan & filter failed videos\n"
            "/forward - Forward messages to target channel\n"
            "/cancel - Cancel current operation"
        )

    @bot.on(events.NewMessage(pattern='/cancel'))
    async def cancel_h(event):
        user = get_user(event.sender_id)
        cleanup_all_files(user)
        user['step'] = None
        user['fwd_step'] = None
        await event.respond("❌ **Cancelled!**")

    @bot.on(events.NewMessage(pattern='/filter'))
    async def filter_cmd(event):
        u = get_user(event.sender_id)
        u['step'] = 'wait_txt'
        u['fwd_step'] = None
        u['txt_lines'] = []
        u['txt_filename'] = ''
        u['file_path'] = None
        u['channel_id'] = None
        u['first_msg_id'] = None
        u['last_msg_id'] = None
        await event.respond("📄 **Send your .txt file**")

    @bot.on(events.NewMessage(pattern='/forward'))
    async def forward_cmd(event):
        u = get_user(event.sender_id)
        u['fwd_step'] = 'wait_target'
        u['step'] = None
        u['fwd_txt_lines'] = []
        u['fwd_txt_filename'] = ''
        u['fwd_file_path'] = None
        u['fwd_target_channel'] = None
        u['fwd_target_id_no_prefix'] = None
        u['fwd_source_channel'] = None
        u['fwd_first_msg_id'] = None
        u['fwd_last_msg_id'] = None
        await event.respond(
            "1️⃣ **Send Target Channel ID**\n"
            "Example: `3444666206` or `-1003444666206`"
        )

    @bot.on(events.NewMessage)
    async def handler(event):
        if event.text and event.text.startswith('/'):
            return
        u = get_user(event.sender_id)

        # ========= FILTER FLOW =========
        if u['step'] == 'wait_txt':
            if event.document:
                fname = get_file_name(event.document)  # ✅ SAFE
                if fname.endswith('.txt'):
                    pth = await event.download_media(
                        file=f'./downloads/{event.sender_id}_{fname}'
                    )
                    with open(pth, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = [l.rstrip('\n').rstrip('\r') for l in f.readlines()]
                    u['txt_lines'] = lines
                    u['txt_filename'] = fname
                    u['file_path'] = pth
                    u['step'] = 'wait_flink'

                    output_name = get_output_filename(fname)
                    await event.respond(
                        f"✅ **File received!**\n"
                        f"📁 Name: `{fname}`\n"
                        f"📝 Lines: **{len(lines)}**\n"
                        f"📤 Output: `{output_name}`\n\n"
                        f"🔗 **Send First Msg Link**\n"
                        f"`https://t.me/c/channel_id/msg_id`"
                    )
                else:
                    await event.respond("❌ **.txt file only!**")
            else:
                await event.respond("❌ **Send a .txt file!**")
            return

        if u['step'] == 'wait_flink':
            cid, mid = parse_link(event.text or '')
            if cid:
                u['channel_id'] = cid
                u['first_msg_id'] = mid
                u['step'] = 'wait_llink'
                await event.respond(
                    f"✅ **First: `-100{cid}` / `{mid}`**\n\n"
                    f"🔗 **Send Last Msg Link**"
                )
            else:
                await event.respond("❌ **Invalid link!**")
            return

        if u['step'] == 'wait_llink':
            cid, mid = parse_link(event.text or '')
            if cid and cid == u['channel_id']:
                u['last_msg_id'] = mid
                u['step'] = 'processing'
                await event.respond(
                    f"✅ **Last: `{mid}`**\n"
                    f"⚙️ **Processing started...**"
                )
                await process_filter(bot, event, u)
            else:
                await event.respond("❌ **Invalid link or channel mismatch!**")
            return

        # ========= FORWARD FLOW =========
        if u['fwd_step'] == 'wait_target':
            full, disp = parse_target_channel(event.text or '')
            if full:
                u['fwd_target_channel'] = full
                u['fwd_target_id_no_prefix'] = disp
                u['fwd_step'] = 'wait_fwd_txt'
                await event.respond(
                    f"✅ **Target Set: `{disp}`**\n\n"
                    f"📄 **Step 2:** Send your `.txt` file"
                )
            else:
                await event.respond("❌ **Invalid Channel ID!**")
            return

        if u['fwd_step'] == 'wait_fwd_txt':
            if event.document:
                fname = get_file_name(event.document)  # ✅ SAFE
                if fname.endswith('.txt'):
                    pth = await event.download_media(
                        file=f'./downloads/{event.sender_id}_fwd_{fname}'
                    )
                    with open(pth, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = [l.rstrip('\n').rstrip('\r') for l in f.readlines()]
                    u['fwd_txt_lines'] = lines
                    u['fwd_txt_filename'] = fname
                    u['fwd_file_path'] = pth
                    u['fwd_step'] = 'wait_src_flink'
                    await event.respond(
                        f"✅ **TXT Received!**\n"
                        f"📁 Name: `{fname}`\n"
                        f"📝 Lines: **{len(lines)}**\n\n"
                        f"🔗 **Step 3:** Send Source First Link\n"
                        f"`https://t.me/c/channel_id/msg_id`"
                    )
                else:
                    await event.respond("❌ **.txt file only!**")
            else:
                await event.respond("❌ **Send a .txt file!**")
            return

        if u['fwd_step'] == 'wait_src_flink':
            cid, mid = parse_link(event.text or '')
            if cid:
                u['fwd_source_channel'] = f"-100{cid}"
                u['fwd_first_msg_id'] = mid
                u['fwd_step'] = 'wait_src_llink'
                await event.respond(
                    f"✅ **Source First: `-100{cid}` / `{mid}`**\n\n"
                    f"🔗 **Step 4:** Send Source Last Link"
                )
            else:
                await event.respond("❌ **Invalid link!**")
            return

        if u['fwd_step'] == 'wait_src_llink':
            cid, mid = parse_link(event.text or '')
            if cid and f"-100{cid}" == u['fwd_source_channel']:
                u['fwd_last_msg_id'] = mid
                u['fwd_step'] = 'run'
                await event.respond(
                    f"✅ **Last: `{mid}`**\n"
                    f"🚀 **Starting Forward...**"
                )
                await process_forward(bot, event, u)
            else:
                await event.respond("❌ **Source channel mismatch!**")
            return

    print("✅ Bot Running...")
    await bot.run_until_disconnected()


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped!")
    finally:
        loop.close()
