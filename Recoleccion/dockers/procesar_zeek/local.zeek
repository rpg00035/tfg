@load base/frameworks/logging
@load base/protocols/conn          # Conn::Info y connection_state_remove

# ── Parámetros ─────────────────────────────────────────────────────────
const LTM_WINDOW_SIZE = 100;       # Tamaño FIFO

# ── Tipo auxiliar con lo imprescindible ───────────────────────────────
type RecentConnInfo: record {
    uid         : string;
    orig_h      : addr;
    resp_h      : addr;
    orig_p      : port;
    resp_p      : port;
    service_str : string;
};

# ── Ventanas LTM (todas empiezan vacías) ───────────────────────────────
global w_src        : vector of RecentConnInfo = vector();
global w_dst        : vector of RecentConnInfo = vector();
global w_srv_src    : vector of RecentConnInfo = vector();
global w_srv_dst    : vector of RecentConnInfo = vector();
global w_src_dport  : vector of RecentConnInfo = vector();
global w_dst_sport  : vector of RecentConnInfo = vector();
global w_dst_src    : vector of RecentConnInfo = vector();

# ── Campos nuevos en conn.log ──────────────────────────────────────────
redef record Conn::Info += {
    ct_srv_src_custom      : count &log &default=0;
    ct_srv_dst_custom      : count &log &default=0;
    ct_dst_ltm_custom      : count &log &default=0;
    ct_src_ltm_custom      : count &log &default=0;
    ct_src_dport_ltm_custom: count &log &default=0;
    ct_dst_sport_ltm_custom: count &log &default=0;
    ct_dst_src_ltm_custom  : count &log &default=0;
};

# ── Funciones auxiliares ───────────────────────────────────────────────
function service_string(c: connection): string
    {
    if ( c$conn?$service && |c$conn$service| > 0 )
        for ( s in c$conn$service ) return s;                # primer servicio etiquetado
    return fmt("%s/%s", c$id$resp_p, c$id$proto);
    }

function add_window(v: vector of RecentConnInfo, r: RecentConnInfo): vector of RecentConnInfo
    {
    v += r;                              # “append” idiomático :contentReference[oaicite:0]{index=0}
    if ( |v| > LTM_WINDOW_SIZE )
        v = v[1:];                       # corta sólo el más antiguo
    return v;
    }

function count_window(win: vector of RecentConnInfo,
                      cur: RecentConnInfo, key: string): count
    {
    if ( |win| == 0 ) return 0;          # vector vacío; nada que contar
    local n: count = 0;

    for ( idx in win )                   # ¡en vectores el bucle devuelve índices! :contentReference[oaicite:1]{index=1}
        {
        local rec = win[idx];
        if ( rec$uid == cur$uid ) next;  # evita contarme a mí mismo

        if ( key == "ct_src_ltm"        && rec$orig_h == cur$orig_h )                             ++n;
        else if ( key == "ct_dst_ltm"   && rec$resp_h == cur$resp_h )                             ++n;
        else if ( key == "ct_srv_src"   && rec$orig_h == cur$orig_h && rec$service_str == cur$service_str ) ++n;
        else if ( key == "ct_srv_dst"   && rec$resp_h == cur$resp_h && rec$service_str == cur$service_str ) ++n;
        else if ( key == "ct_src_dport_ltm" && rec$orig_h == cur$orig_h && rec$resp_p == cur$resp_p )       ++n;
        else if ( key == "ct_dst_sport_ltm" && rec$resp_h == cur$resp_h && rec$orig_p == cur$orig_p )       ++n;
        else if ( key == "ct_dst_src_ltm"   && rec$orig_h == cur$orig_h && rec$resp_h == cur$resp_h )       ++n;
        }
    return n;
    }

# ── Evento principal ──────────────────────────────────────────────────
event connection_state_remove(c: connection)
    {
    if ( !c?$conn ) return;              # seguridad extra

    local svc = service_string(c);
    local cur: RecentConnInfo = [$uid=c$uid,
                                 $orig_h=c$id$orig_h, $resp_h=c$id$resp_h,
                                 $orig_p=c$id$orig_p, $resp_p=c$id$resp_p,
                                 $service_str=svc];

    # Calcula las 7 features
    c$conn$ct_src_ltm_custom       = count_window(w_src,        cur, "ct_src_ltm");
    c$conn$ct_dst_ltm_custom       = count_window(w_dst,        cur, "ct_dst_ltm");
    c$conn$ct_srv_src_custom       = count_window(w_srv_src,    cur, "ct_srv_src");
    c$conn$ct_srv_dst_custom       = count_window(w_srv_dst,    cur, "ct_srv_dst");
    c$conn$ct_src_dport_ltm_custom = count_window(w_src_dport,  cur, "ct_src_dport_ltm");
    c$conn$ct_dst_sport_ltm_custom = count_window(w_dst_sport,  cur, "ct_dst_sport_ltm");
    c$conn$ct_dst_src_ltm_custom   = count_window(w_dst_src,    cur, "ct_dst_src_ltm");

    # Rota las ventanas (FIFO)
    w_src       = add_window(w_src,       cur);
    w_dst       = add_window(w_dst,       cur);
    w_srv_src   = add_window(w_srv_src,   cur);
    w_srv_dst   = add_window(w_srv_dst,   cur);
    w_src_dport = add_window(w_src_dport, cur);
    w_dst_sport = add_window(w_dst_sport, cur);
    w_dst_src   = add_window(w_dst_src,   cur);
    }
