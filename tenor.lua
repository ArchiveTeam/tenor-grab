local urlparse = require("socket.url")
local http = require("socket.http")
local cjson = require("cjson")
local utf8 = require("utf8")
local base64 = require("base64")
local basex = require("basex")
local bignum = require("openssl.bignum")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local context = nil
local cached_api_data = {}

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_items = {}
local discovered_media = {}
local discovered_count = 0
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true
local base62 = basex("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

local item_patterns = {
  ["^https?://tenor%.com/view/gif%-([0-9]+)$"]="post",
  ["^https?://tenor%.com/view/[^/%?#]+%-gif%-([0-9]+)$"]="post",
  ["^https?://tenor%.com/embed/([0-9]+)"]="post",
  ["^https?://tenor%.googleapis%.com/v2/posts%?ids=([0-9]+)"]="post",
  ["^https?://tenor%.com/users/([^/%?#]+)"]="user",
  ["^https?://tenor%.com/official/([^/%?#]+)"]="user",
  ["^https?://tenor%.googleapis%.com/v2/user%?username=([^&]+)"]="user",
  ["^https?://tenor%.com/search/([^/%?#]+)%-gifs"]="search",
  ["^https?://tenor%.com/search/([^/%?#]+)%-stickers"]="search",
  ["^https?://tenor%.com/search/([^/%?#]+)%-memes"]="search",
  ["^https?://tenor%.com/%?suggest=([^&]+)&locale=([^&]+)"]="suggest",
  ["^https?://tenor%.googleapis%.com/v2/search%?q=([^&]+)&locale=([^&]+)&"]="search",
  ["^https?://tenor%.googleapis%.com/v2/search_suggestions%?q=([^&]+)&locale=([^&]+)&"]="suggest",
  ["^https?://tenor%.googleapis%.com/v2/autocomplete%?q=([^&]+)&locale=([^&]+)&"]="suggest"
}

local countries = {
  ["af"]=true,
  ["am"]=true,
  ["az"]=true,
  ["be"]=true,
  ["bg"]=true,
  ["bn"]=true,
  ["bs"]=true,
  ["ca"]=true,
  ["cs"]=true,
  ["da"]=true,
  ["de"]=true,
  ["de-AT"]=true,
  ["de-CH"]=true,
  ["el"]=true,
  ["en-AU"]=true,
  ["en-CA"]=true,
  ["en-GB"]=true,
  ["en-IE"]=true,
  ["en-IN"]=true,
  ["en-NZ"]=true,
  ["en-SG"]=true,
  ["en-ZA"]=true,
  ["es"]=true,
  ["es-419"]=true,
  ["es-AR"]=true,
  ["es-BO"]=true,
  ["es-CL"]=true,
  ["es-CO"]=true,
  ["es-CR"]=true,
  ["es-DO"]=true,
  ["es-EC"]=true,
  ["es-GT"]=true,
  ["es-HN"]=true,
  ["es-MX"]=true,
  ["es-NI"]=true,
  ["es-PA"]=true,
  ["es-PE"]=true,
  ["es-PR"]=true,
  ["es-PY"]=true,
  ["es-SV"]=true,
  ["es-US"]=true,
  ["es-UY"]=true,
  ["es-VE"]=true,
  ["et"]=true,
  ["eu"]=true,
  ["fi"]=true,
  ["fil"]=true,
  ["fr"]=true,
  ["fr-CA"]=true,
  ["fr-CH"]=true,
  ["gl"]=true,
  ["gu"]=true,
  ["hi"]=true,
  ["hr"]=true,
  ["hu"]=true,
  ["hy"]=true,
  ["id"]=true,
  ["is"]=true,
  ["it"]=true,
  ["ja"]=true,
  ["ka"]=true,
  ["kk"]=true,
  ["km"]=true,
  ["kn"]=true,
  ["ko"]=true,
  ["ky"]=true,
  ["lo"]=true,
  ["lt"]=true,
  ["lv"]=true,
  ["mk"]=true,
  ["ml"]=true,
  ["mn"]=true,
  ["mo"]=true,
  ["mr"]=true,
  ["ms"]=true,
  ["my"]=true,
  ["ne"]=true,
  ["nl"]=true,
  ["no"]=true,
  ["pa"]=true,
  ["pl"]=true,
  ["pt"]=true,
  ["pt-BR"]=true,
  ["pt-PT"]=true,
  ["ro"]=true,
  ["ru"]=true,
  ["si"]=true,
  ["sk"]=true,
  ["sl"]=true,
  ["sq"]=true,
  ["sr"]=true,
  ["sr-Latn"]=true,
  ["sv"]=true,
  ["sw"]=true,
  ["ta"]=true,
  ["te"]=true,
  ["th"]=true,
  ["tl"]=true,
  ["tr"]=true,
  ["uk"]=true,
  ["uz"]=true,
  ["vi"]=true,
  ["zh-CN"]=true,
  ["zh-HK"]=true,
  ["zh-TW"]=true,
  ["zu"]=true
}

local country_paths = {
  ["gif-maker"]=true,
  ["explore"]=true,
  ["official"]=true,
  ["search"]=true,
  ["users"]=true,
  ["view"]=true
}

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  io.stdout:flush()
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file, "rb"))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

queue_discovered_items = function()
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end
  for key, data in pairs({
    ["tenor-4d8e594bdb640081"] = discovered_items,
    ["tenor-media-7ea0673f9a6e0fd5"] = discovered_media
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    local queued = {}
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      table.insert(queued, item)
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        for _, item in ipairs(queued) do
          data[item] = nil
        end
        discovered_count = discovered_count - count
        items = nil
        count = 0
        queued = {}
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
      for _, item in ipairs(queued) do
        data[item] = nil
      end
      discovered_count = discovered_count - count
    end
  end
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
    discovered_count = discovered_count + 1
    if discovered_count >= 1000 then
      queue_discovered_items()
    end
    return true
  end
  return false
end

escape_item_value = function(value)
  return string.gsub(
    urlparse.escape(urlparse.unescape(value)),
    "%%[0-9a-fA-F][0-9a-fA-F]",
    string.upper
  )
end

find_item = function(url)
  for pattern, name in pairs(item_patterns) do
    local value, value2 = string.match(url, pattern)
    if value then
      if name == "search"
        or name == "suggest" then
        value = escape_item_value(value)
        value = (value2 or "en") .. ":" .. value
      elseif name == "user" then
        value = escape_item_value(value)
      end
      return {
        ["value"]=value,
        ["type"]=name
      }
    end
  end
end

is_same_item = function(type_, value)
  return type_ == item_type
    and (
      ids[string.lower(value)]
      or ids[string.lower(urlparse.unescape(value))]
      or ids[string.lower(urlparse.escape(value))]
      or ids[string.lower(string.match(value, "([^:]+)$"))]
    )
end

check_item_done = function()
  if context["pending_search_api"] then
    error("Pending search API URLs were not queued.")
  end
  if item_type == "post"
    and not context["skip_item"]
    and not abortgrab
    and not context["media_item"] then
    error("No media item found.")
  end
end

set_item = function(url)
  if ids[string.lower(url)] then
    return nil
  end
  local found = find_item(url)
  if found then
    local newcontext = {}
    local new_item_type = found["type"]
    local new_item_value = found["value"]
    local new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name
      and not is_same_item(new_item_type, new_item_value) then
      if item_name then
        check_item_done()
      end
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      if new_item_type == "search"
        or new_item_type == "suggest" then
        local locale, value = string.match(item_value, "^([^:]+):(.+)$")
        context["locale"] = locale
        local unescaped_value = urlparse.unescape(value)
        ids[string.lower(unescaped_value)] = true
        ids[string.lower(string.gsub(unescaped_value, "%s+", "-"))] = true
        if new_item_type == "search" then
          context["term"] = value
          context["search_path"] = escape_item_value(string.gsub(unescaped_value, "%s+", "-"))
        elseif new_item_type == "suggest" then
          context["prefix"] = value
        end
      elseif new_item_type == "user" then
        local unescaped_value = urlparse.unescape(item_value)
        ids[string.lower(unescaped_value)] = true
        ids[string.lower("@" .. item_value)] = true
        ids[string.lower("@" .. unescaped_value)] = true
      end
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  local temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  if string.match(url, "[%s\\]")
    or string.match(url, "^https?://tenor%.googleapis%.com/v2/register")
    or string.match(url, "^https?://tenor%.com/gif%-maker[%?/]") then
    return false
  end

  local country, path = string.match(url, "^https?://tenor%.com/([0-9A-Za-z%-]+)/([^/%?]*)[/%?]")
  if countries[country]
    and (
      path == ""
      or country_paths[path]
    ) then
    return false
  end

  if ids[url] or ids[string.lower(url)] then
    return true
  end

  local found = find_item(url)
  if found then
    local found_item_name = found["type"] .. ":" .. found["value"]
    if is_same_item(found["type"], found["value"]) then
      return true
    end
    if found_item_name ~= item_name then
      discover_item(discovered_items, found_item_name)
      return false
    end
    return true
  end

  local host = string.match(url, "^https?://([^/:]+)")
  if host
    and host ~= "tenor.com"
    and host ~= "tenor.googleapis.com"
    and host ~= "media.tenor.com"
    and host ~= "media1.tenor.com"
    and host ~= "c.tenor.com" then
    return false
  end

  for _, pattern in pairs({
    "([0-9]+)",
    "([0-9a-zA-Z_%%%.%-]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      s = urlparse.unescape(s)
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  set_item(url)

  if abortgrab then
    return urls
  end
  if context["skip_item"] then
    return urls
  end
  downloaded[url] = true

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl, headers, body_data, method)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://") then
      return nil
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = url
    while string.match(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    local key = (method or "GET") .. "\0" .. url_ .. "\0" .. tostring(body_data)
    if not processed(key)
      and (
        body_data
        or not processed(url_)
      )
      and allowed(url_, origurl) then
      headers = headers or {}
      if string.match(url_, "^https?://tenor%.googleapis%.com/")
        and not string.match(url_, "[%?&]key=") then
        if not context["api_key"] then
          error("No API key available.")
        end
        headers["x-goog-api-key"] = context["api_key"]
      elseif not string.match(url_, "^https?://tenor%.googleapis%.com/") then
        headers["x-goog-api-key"] = ""
      end
      local url_data = {
        url=url_,
        headers=headers
      }
      if body_data then
        url_data["body_data"] = body_data
        url_data["method"] = method or "POST"
      end
      table.insert(urls, url_data)
      addedtolist[key] = true
      if not body_data then
        addedtolist[url_] = true
        addedtolist[url] = true
      end
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function api_v2_query(locale)
    return "?appversion=browser-" .. context["release_short_tag"]
      .. "&prettyPrint=false"
      .. "&key=" .. context["api_key"]
      .. "&client_key=" .. context["client_key"]
      .. "&locale=" .. locale
  end

  local function check_api_v2(path, query)
    check(
      context["api_url"] .. path .. "?" .. query
      .. "&key=" .. context["api_key"]
      .. "&client_key=" .. context["client_key"]
    )
  end

  local function check_search_api(term, locale, searchfilter, pos)
    if not context["anon_id"] then
      context["pending_search_api"] = context["pending_search_api"] or {}
      table.insert(context["pending_search_api"], {term, locale, searchfilter, pos})
      return nil
    end
    local user_search = string.match(urlparse.unescape(term), "^@")
    local newurl = "https://tenor.googleapis.com/v2/search"
      .. api_v2_query(locale)
      .. "&anon_id=" .. context["anon_id"]
    if not user_search then
      newurl = newurl .. "&q=" .. term .. "&limit=50&contentfilter=low"
    end
    newurl = newurl
      .. "&media_filter=gif%2Cgif_transparent%2Cmediumgif%2Ctinygif"
      .. "%2Ctinygif_transparent%2Cwebp%2Cwebp_transparent%2Ctinywebp"
      .. "%2Ctinywebp_transparent%2Ctinymp4%2Cmp4%2Cwebm"
      .. "%2Coriginalgif%2Cgifpreview"
      .. "&fields=next%2Cresults.id%2Cresults.media_formats"
      .. "%2Cresults.title%2Cresults.h1_title%2Cresults.long_title"
      .. "%2Cresults.itemurl%2Cresults.url%2Cresults.created"
      .. "%2Cresults.user%2Cresults.shares%2Cresults.embed"
      .. "%2Cresults.hasaudio%2Cresults.policy_status"
      .. "%2Cresults.source_id%2Cresults.flags%2Cresults.tags"
      .. "%2Cresults.content_rating%2Cresults.bg_color"
      .. "%2Cresults.legacy_info%2Cresults.geographic_restriction"
      .. "%2Cresults.content_description"
    if user_search then
      if pos then
        newurl = newurl .. "&pos=" .. pos
      end
      newurl = newurl .. "&q=" .. term .. "&limit=50"
    end
    if searchfilter then
      newurl = newurl .. "&searchfilter=" .. searchfilter
    end
    if pos and not user_search then
      newurl = newurl .. "&pos=" .. pos
    end
    newurl = newurl .. "&component=web_desktop"
    check(newurl)
  end

  local function check_short_gif(post_id)
    local newurl = "https://tenor.com/" .. base62:encode(bignum.new(tostring(post_id)):toBinary()) .. ".gif"
    ids[string.lower(newurl)] = true
    check(newurl)
  end

  local function process_post(post)
    local post_id = post["id"]
    local legacy_id = nil
    if type(post["legacy_info"]) == "table"
      and type(post["legacy_info"]["post_id"]) == "string" then
      legacy_id = post["legacy_info"]["post_id"]
    end
    if item_type == "post"
      and ids[string.lower(post_id)] then
      check("https://tenor.com/view/gif-" .. post_id)
      check("https://tenor.com/embed/" .. post_id)
      check(context["api_url"] .. "/posts?ids=" .. post_id)
      check(post["itemurl"])
      ids[string.lower(post["url"])] = true
      check(post["url"])
      check_short_gif(post_id)
      local oembed_targets = {
        "https://tenor.com/view/gif-" .. post_id,
        post["itemurl"]
      }
      if legacy_id
        and legacy_id ~= "0" then
        ids[string.lower(legacy_id)] = true
        check("https://tenor.com/view/gif-" .. legacy_id)
        check("https://tenor.com/embed/" .. legacy_id)
        check(context["api_url"] .. "/posts?ids=" .. legacy_id)
        check_short_gif(legacy_id)
        table.insert(oembed_targets, "https://tenor.com/view/gif-" .. legacy_id)
        local long_itemurl = string.gsub(
          post["itemurl"],
          "%-gif%-" .. legacy_id .. "$",
          "-gif-" .. post_id
        )
        check(long_itemurl)
        table.insert(oembed_targets, long_itemurl)
      end
      for _, oembed_target in pairs(oembed_targets) do
        oembed_target = string.gsub(oembed_target, ":", "%%3A")
        oembed_target = string.gsub(oembed_target, "/", "%%2F")
        local oembed_url = "https://tenor.com/oembed?url=" .. oembed_target
        check(oembed_url)
        check(oembed_url .. "&format=xml")
      end
      if not context["media_item"] then
        local function sorted_keys(data)
          local keys = {}
          for key, _ in pairs(data) do
            table.insert(keys, key)
          end
          table.sort(keys)
          return keys
        end
        local rows = {}
        for _, name in ipairs(sorted_keys(post["media_formats"])) do
          local media = post["media_formats"][name]
          local media_rows = {}
          for _, key in ipairs(sorted_keys(media)) do
            if key ~= "duration" then
              table.insert(media_rows, {key, media[key]})
            end
          end
          table.insert(rows, {name, media_rows})
        end
        local input_file = item_dir .. "/" .. warc_file_base .. "_media.json"
        local output_file = item_dir .. "/" .. warc_file_base .. "_media.zst"
        local file = assert(io.open(input_file, "wb"))
        file:write(cjson.encode(rows))
        file:close()
        if os.execute(
          "zstd -q -f -19 --check -D media-zst-dict.bin -o "
          .. output_file .. " " .. input_file
        ) ~= 0 then
          os.remove(input_file)
          os.remove(output_file)
          error("Failed to compress media item.")
        end
        local value = base64.encode(read_file(output_file))
        os.remove(input_file)
        os.remove(output_file)
        value = string.gsub(value, "%+", "-")
        value = string.gsub(value, "/", "_")
        value = string.gsub(value, "=+$", "")
        context["media_item"] = "media:" .. value
        discover_item(discovered_media, context["media_item"])
      end
      if type(post["tags"]) == "table" then
        for _, tag in ipairs(post["tags"]) do
          discover_item(discovered_items, "search:en:" .. escape_item_value(tag))
        end
      end
    end
    discover_item(discovered_items, "post:" .. post_id)
    if type(post["copied_post_pid"]) == "string"
      and post["copied_post_pid"] ~= "0" then
      discover_item(discovered_items, "post:" .. post["copied_post_pid"])
    end
    if type(post["user"]) == "table" then
      discover_item(discovered_items, "user:" .. escape_item_value(post["user"]["username"]))
    end
  end

  local function discover_json(json_data)
    if type(json_data) == "table" then
      if json_data["media_formats"]
        and json_data["id"] then
        process_post(json_data)
      elseif json_data["username"]
        and json_data["url"] then
        local username = json_data["username"]
        if is_same_item("user", username) then
          for _, group in pairs({
            json_data["avatars"],
            json_data["partnerbanner"]
          }) do
            for _, newurl in pairs(group) do
              ids[string.lower(newurl)] = true
              check(newurl)
            end
          end
          check(json_data["url"])
          check(json_data["url"] .. "/stickers")
          check(context["api_url"] .. "/user?username=" .. escape_item_value(username))
          check_search_api(escape_item_value("@" .. username), "en")
          check_search_api(escape_item_value("@" .. username), "en", "sticker")
        end
        discover_item(discovered_items, "user:" .. escape_item_value(username))
      elseif json_data["searchterm"] then
        discover_item(discovered_items, "search:en:" .. escape_item_value(json_data["searchterm"]))
        if json_data["id"]
          and string.match(json_data["id"], "^[0-9]+$") then
          discover_item(discovered_items, "post:" .. json_data["id"])
        end
      end
      for _, value in pairs(json_data) do
        discover_json(value)
      end
    end
  end

  if allowed(url)
    and status_code == 200 then
    html = read_file(file)
    if string.match(url, "^https?://tenor%.googleapis%.com/v2/") then
      json = cjson.decode(html)
      if string.match(url, "^https?://tenor%.googleapis%.com/v2/anonid%?") then
        context["anon_id"] = json["anon_id"]
        cached_api_data["anon_id"] = context["anon_id"]
        local app_config_url = context["api_url"] .. "/app_config"
          .. api_v2_query("en") .. "&anon_id=" .. context["anon_id"]
        ids[string.lower(app_config_url)] = true
        check(app_config_url)
        if context["pending_search_api"] then
          for _, data in ipairs(context["pending_search_api"]) do
            check_search_api(data[1], data[2], data[3], data[4])
          end
          context["pending_search_api"] = nil
        end
      end
      if (item_type == "search" or item_type == "user")
        and string.match(url, "^https?://tenor%.googleapis%.com/v2/search%?") then
        local term = string.match(url, "[%?&]q=([^&]+)")
        local locale = string.match(url, "[%?&]locale=([^&]+)")
        local next_pos = json["next"]
        if item_type == "user"
          and string.lower(urlparse.unescape(term)) ~= "@" .. string.lower(urlparse.unescape(item_value)) then
          term = nil
        end
        if term
          and type(next_pos) == "string"
          and string.len(next_pos) > 0 then
          check_search_api(
            term,
            locale,
            string.match(url, "[%?&]searchfilter=([^&]+)"),
            next_pos
          )
        end
      end
      if item_type == "suggest"
        and string.match(url, "^https?://tenor%.googleapis%.com/v2/search_suggestions%?") then
        for _, term in ipairs(json["results"]) do
          discover_item(discovered_items, "search:en:" .. escape_item_value(term))
        end
        check_api_v2(
          "/autocomplete",
          "q=" .. context["prefix"] .. "&locale=" .. context["locale"] .. "&limit=50"
        )
      end
      discover_json(json)
    elseif string.match(url, "^https?://tenor%.com/") then
      for data in string.gmatch(html, '<script id="data"[^>]*>(.-)</script>') do
        data = cjson.decode(base64.decode(data))
        context["api_key"] = data["API_V2_KEY"]
        context["api_url"] = data["API_V2_URL"]
        context["client_key"] = data["API_V2_CLIENT_KEY"]
        context["release_short_tag"] = data["RELEASE_SHORT_TAG"]
        if cached_api_data["api_key"] == context["api_key"]
          and cached_api_data["api_url"] == context["api_url"]
          and cached_api_data["client_key"] == context["client_key"]
          and cached_api_data["release_short_tag"] == context["release_short_tag"] then
          context["anon_id"] = cached_api_data["anon_id"]
        else
          cached_api_data["anon_id"] = nil
        end
        cached_api_data["api_key"] = context["api_key"]
        cached_api_data["api_url"] = context["api_url"]
        cached_api_data["client_key"] = context["client_key"]
        cached_api_data["release_short_tag"] = context["release_short_tag"]
        if not context["anon_id"] then
          local anonid_url = context["api_url"] .. "/anonid" .. api_v2_query("en")
          ids[string.lower(anonid_url)] = true
          check(anonid_url)
        end
      end
      for _, script in ipairs({
        '<script id="store%-cache"[^>]*>(.-)</script>',
        '<script id="gif%-json"[^>]*>(.-)</script>'
      }) do
        for data in string.gmatch(html, script) do
          discover_json(cjson.decode(data))
        end
      end
      if item_type == "search" then
        for _, suffix in pairs({
          "gifs",
          "stickers",
          "memes"
        }) do
          check("https://tenor.com/search/" .. context["search_path"] .. "-" .. suffix)
        end
        check_search_api(context["term"], context["locale"])
        for _, searchfilter in pairs({
          "none",
          "sticker",
          "static%2C-sticker"
        }) do
          check_search_api(context["term"], context["locale"], searchfilter)
        end
      elseif item_type == "user" then
        check(context["api_url"] .. "/user?username=" .. escape_item_value(item_value))
      elseif item_type == "suggest" then
        check_api_v2(
          "/search_suggestions",
          "q=" .. context["prefix"] .. "&locale=" .. context["locale"] .. "&limit=50"
        )
      end
      for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
        checknewurl(newurl)
      end
      for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
        checknewurl(newurl)
      end
      for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
        checknewshorturl(newurl)
      end
      for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
        checknewshorturl(newurl)
      end
      for newurl in string.gmatch(html, "[^%-]src='([^']+)'") do
        checknewshorturl(newurl)
      end
      for newurl in string.gmatch(html, '[^%-]src="([^"]+)"') do
        checknewshorturl(newurl)
      end
      html = string.gsub(html, "&gt;", ">")
      html = string.gsub(html, "&lt;", "<")
      for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
        checknewurl(newurl)
      end
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  if http_stat["len"] == 0
    and status_code == 200 then
    retry_url = true
    return false
  end
  local item_url = url["url"]
  if status_code == 400
    or status_code == 404 then
    abort_item()
    return false
  end
  local function find_new_post_id(data)
    if type(data) == "table" then
      if type(data["legacy_info"]) == "table"
        and data["legacy_info"]["post_id"] == item_value
        and data["id"] ~= item_value then
        return data["id"]
      end
      for _, value in pairs(data) do
        local post_id = find_new_post_id(value)
        if post_id then
          return post_id
        end
      end
    end
  end
  if item_type == "post"
    and status_code == 200
    and (
      string.match(item_url, "^https?://tenor%.googleapis%.com/v2/posts%?")
      or string.match(item_url, "^https?://tenor%.com/view/")
      or string.match(item_url, "^https?://tenor%.com/embed/")
    ) then
    local body = read_file(http_stat["local_file"])
    local json = nil
    local new_post_id = nil
    if string.match(item_url, "^https?://tenor%.googleapis%.com/v2/posts%?") then
      json = cjson.decode(body)
      new_post_id = find_new_post_id(json)
    else
      for _, script in ipairs({
        '<script id="data"[^>]*>(.-)</script>',
        '<script id="store%-cache"[^>]*>(.-)</script>',
        '<script id="gif%-json"[^>]*>(.-)</script>'
      }) do
        for data in string.gmatch(body, script) do
          if not string.match(data, "^%s*[%[{]") then
            data = base64.decode(data)
          end
          new_post_id = find_new_post_id(cjson.decode(data))
          if new_post_id then
            break
          end
        end
        if new_post_id then
          break
        end
      end
    end
    if new_post_id then
      discover_item(discovered_items, "post:" .. new_post_id)
      context["skip_item"] = true
      io.stdout:write("Not writing legacy post response to WARC.\n")
      io.stdout:flush()
      return false
    end
    if not (
      string.match(item_url, "^https?://tenor%.googleapis%.com/v2/posts%?ids=" .. item_value .. "$")
      or string.match(item_url, "^https?://tenor%.googleapis%.com/v2/posts%?ids=" .. item_value .. "&")
    ) then
      return true
    end
    local matched_post = nil
    for _, post in ipairs(json["results"]) do
      if post["id"] == item_value then
        matched_post = post
        break
      end
    end
    if not matched_post then
      abort_item()
      return false
    end
    local media_format_count = 0
    if matched_post["media_formats"] then
      for _, _ in pairs(matched_post["media_formats"]) do
        media_format_count = media_format_count + 1
      end
    end
    if media_format_count == 0 then
      io.stdout:write("No media formats found.\n")
      io.stdout:flush()
      abort_item()
      return false
    end
  end
  if status_code ~= 200
    and status_code ~= 301
    and status_code ~= 302 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if context["skip_item"] then
    return wget.actions.EXIT
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 6
    if status_code == 401 or status_code == 403 or status_code == 404 then
      tries = maxtries + 1
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 or status_code == 206 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if item_type == "post" then
      local redirected_post_id = string.match(newloc, "^https?://tenor%.com/view/gif%-([0-9]+)$")
        or string.match(newloc, "^https?://tenor%.com/view/[^/%?#]+%-gif%-([0-9]+)$")
      if redirected_post_id then
        ids[string.lower(redirected_post_id)] = true
      end
    end
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  check_item_done()
  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  queue_discovered_items()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end
