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

load_dotenv()

API_ID = int(os.getenv("API_ID", "23713783"))
API_HASH = os.getenv("API_HASH", "2daa157943cb2d76d149c4de0b036a99")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7213717609:AAGEyAJSfMUderWqlIAJkziRcIBrVTwjbXM")
PORT = int(os.getenv("PORT", 8080))

user_data = {}


def get_user(user_id):
    if user_id not in user_data:
        user_data[user_id] = {
            'step': None, 'txt_lines': [], 'txt_filename': '', 'file_path': None,
            'channel_id': None, 'first_msg_id': None, 'last_msg_id': None,
            'fwd_step': None, 'fwd_target_channel': None, 'fwd_target_id_no_prefix': None,
            'fwd_txt_lines': [], 'fwd_txt_filename': '', 'fwd_file_path': None,
            'fwd_source_channel': None, 'fwd_source_disp_id': None,
            'fwd_first_msg_id': None, 'fwd_last_msg_id': None,
        }
    return user_data[user_id]


def get_file_name(document):
    if not document:
        return ""
    if hasattr(document, 'attributes'):
        for attr in document.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name
    return ""


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


def parse_link(link):
    match = re.search(r't\.me/c/(\d+)/(\d+)', link)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def parse_target_channel(text):
    text = str(text).strip()
    if text.startswith('-100'):
        numeric = text[4:]
        if numeric.isdigit():
            return int(text), numeric
    elif text.startswith('-'):
        numeric = text[1:]
        if numeric.isdigit():
            return int(f"-100{numeric}"), numeric
    else:
        if text.isdigit():
            return int(f"-100{text}"), text
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


def extract_keys_from_txt_format(txt_lines):
    """
    Extract keys from format: 🌚{number}🌚{name}💀{key}💀 : url
    Returns list of dicts with line info
    """
    results = []
    pattern = re.compile(r'🌚\{([^}]*)\}🌚\{([^}]*)\}💀\{([^}]*)\}💀')

    for idx, line in enumerate(txt_lines):
        stripped = line.strip()
        if not stripped:
            continue
        match = pattern.search(stripped)
        if match:
            number_part = match.group(1).strip()
            name = match.group(2).strip()
            key = match.group(3).strip()
            results.append({
                'line_idx': idx,
                'line_number': number_part,
                'name': name,
                'key': key,
                'original_line': line.rstrip('\n').rstrip('\r')
            })
    return results


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
    source_disp_id = user['fwd_source_disp_id']
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
        f"📥 Source: `{source_disp_id}`\n"
        f"📤 Target: `{target_disp_id}`\n"
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
        missed_lines = set()

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
                missed_lines.add(line_idx + 1)
                if line_idx not in line_mappings:
                    line_mappings[line_idx] = []
                line_mappings[line_idx].append({'qual': quality, 'key': key, 'url': None})
                continue

            src_cap = source_msg.raw_text or source_msg.text or ""
            new_cap = remove_chapter_id_from_caption(src_cap)

            bold_entities = []
            if new_cap:
                bold_entities = [MessageEntityBold(0, len(new_cap))]

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
                missed_lines.add(line_idx + 1)
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
            caption=f"📄 **Updated List** (`{base_name}_updated.txt`)\n✅ Forwarded: **{forwarded_count}**\n❌ Missing: **{not_found_count}**",
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

        missed_msg = ""
        if missed_lines:
            sorted_missed = sorted(list(missed_lines))
            missed_msg = f"⚠️ **Missed Line Numbers:** `{', '.join(map(str, sorted_missed))}`"
            await event.respond(missed_msg)

        try:
            final_msg = f"🎉 **Task Completed!**\n✅ Forwarded: **{forwarded_count}**\n❌ Not Found: **{not_found_count}**"
            if missed_msg:
                final_msg += f"\n{missed_msg}"
            await status_msg.edit(final_msg)
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


# ====================== FILTER PROCESS (KEY BASED) ======================
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

    # Step 1: Extract all keys from txt file
    txt_key_entries = extract_keys_from_txt_format(txt_lines)

    if not txt_key_entries:
        await event.respond(
            "❌ **No keys found in .txt file!**\n\n"
            "Format hona chahiye:\n"
            "`🌚{number}🌚{name}💀{key}💀 : url`"
        )
        user['step'] = None
        cleanup_file(downloaded_file_path)
        user['file_path'] = None
        return

    # Build key -> entry mapping
    all_txt_keys = {}
    for entry in txt_key_entries:
        all_txt_keys[entry['key']] = entry

    found_keys_in_channel = set()
    processed = 0

    status_msg = await event.respond(
        f"🔄 **Scanning Channel...**\n"
        f"📊 Messages: ~{total_messages}\n"
        f"🔑 Keys in TXT: {len(all_txt_keys)}\n"
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

                        # Check if any txt key exists in this message
                        if text:
                            for txt_key in all_txt_keys:
                                if txt_key in text:
                                    found_keys_in_channel.add(txt_key)

                        # Also check document file name
                        if msg and hasattr(msg, 'document') and msg.document:
                            fname = get_file_name(msg.document)
                            if fname:
                                for txt_key in all_txt_keys:
                                    if txt_key in fname:
                                        found_keys_in_channel.add(txt_key)

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
                    found_so_far = len(found_keys_in_channel)
                    missing_so_far = len(all_txt_keys) - found_so_far
                    await status_msg.edit(
                        f"🔄 **Scanning... {pct}%**\n"
                        f"📊 {processed}/{total_messages}\n"
                        f"✅ Found: {found_so_far}/{len(all_txt_keys)}\n"
                        f"❌ Missing: {missing_so_far}\n"
                        f"⏳ Wait..."
                    )
                    last_edit_time = now
                except:
                    pass

        # Step 2: Find missing keys
        missing_entries = []
        for key, entry in all_txt_keys.items():
            if key not in found_keys_in_channel:
                missing_entries.append(entry)

        found_count = len(found_keys_in_channel & set(all_txt_keys.keys()))
        missing_count = len(missing_entries)

        # Step 3: Build output file - ONLY missing lines, nothing extra
        output_display_name = get_output_filename(original_filename)
        output_temp_path = f"./downloads/{event.sender_id}_{output_display_name}"

        with open(output_temp_path, 'w', encoding='utf-8') as f:
            if missing_entries:
                for entry in missing_entries:
                    f.write(entry['original_line'] + '\n')
            else:
                f.write("")

        # Step 4: Send missing line numbers in chat (only idx)
        if missing_entries:
            missing_indices = [entry['line_number'] for entry in missing_entries]
            idx_str = ', '.join(missing_indices)

            # Split if too long
            if len(idx_str) > 3500:
                idx_str = idx_str[:3500] + '...'

            await event.respond(f"❌ **Missing ({missing_count}):**\n`{idx_str}`")

        try:
            await status_msg.edit(
                f"✅ **Scan Complete!**\n\n"
                f"📊 Scanned: **{processed}** msgs\n"
                f"🔑 Total Keys: **{len(all_txt_keys)}**\n"
                f"✅ Found: **{found_count}**\n"
                f"❌ Missing: **{missing_count}**\n"
                f"📄 Uploading `{output_display_name}`..."
            )
        except:
            pass

        caption = (
            f"📄 **{output_display_name}**\n\n"
            f"🔑 Total Keys: **{len(all_txt_keys)}**\n"
            f"✅ Found: **{found_count}**\n"
            f"❌ Missing: **{missing_count}**"
        )

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
            "/filter - Find missing keys from channel\n"
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
        await event.respond(
            "📄 **Send your .txt file**\n\n"
            "Format: `🌚{num}🌚{name}💀{key}💀 : url`"
        )

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
        u['fwd_source_disp_id'] = None
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
                fname = get_file_name(event.document)
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

                    key_entries = extract_keys_from_txt_format(lines)
                    output_name = get_output_filename(fname)

                    await event.respond(
                        f"✅ **File received!**\n"
                        f"📁 Name: `{fname}`\n"
                        f"📝 Lines: **{len(lines)}**\n"
                        f"🔑 Keys: **{len(key_entries)}**\n"
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
                    f"⚙️ **Scanning for missing keys...**"
                )
                await process_filter(bot, event, u)
            else:
                await event.respond("❌ **Invalid link or channel mismatch!**")
            return

        # ========= FORWARD FLOW =========
        if u['fwd_step'] == 'wait_target':
            full_id, disp_id = parse_target_channel(event.text or '')
            if full_id:
                u['fwd_target_channel'] = full_id
                u['fwd_target_id_no_prefix'] = disp_id
                u['fwd_step'] = 'wait_fwd_txt'
                await event.respond(
                    f"✅ **Target Set: `{disp_id}`**\n\n"
                    f"📄 **Step 2:** Send your `.txt` file"
                )
            else:
                await event.respond("❌ **Invalid Channel ID!**")
            return

        if u['fwd_step'] == 'wait_fwd_txt':
            if event.document:
                fname = get_file_name(event.document)
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
                u['fwd_source_channel'] = int(f"-100{cid}")
                u['fwd_source_disp_id'] = str(cid)
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
            if cid:
                expected_source = int(f"-100{cid}")
                if expected_source == u['fwd_source_channel']:
                    u['fwd_last_msg_id'] = mid
                    u['fwd_step'] = 'run'
                    await event.respond(
                        f"✅ **Last: `{mid}`**\n"
                        f"🚀 **Starting Forward...**"
                    )
                    await process_forward(bot, event, u)
                else:
                    await event.respond("❌ **Source channel mismatch!**")
            else:
                await event.respond("❌ **Invalid link!**")
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
