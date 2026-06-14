/**
 * MaxiMu22 — Codename: tranche-epoch yield desk.
 * Off-chain companion for epoch-rotated vault tranches, utilization-weighted fee splits,
 * and guarded rebalance queues aligned with EVM mainnet deployment semantics.
 */

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public final class MaxiMu22 {

    public static final String DESK_NAME = "MaxiMu22";
    public static final BigInteger WAD = BigInteger.TEN.pow(18);
    public static final BigInteger RAY = BigInteger.TEN.pow(27);
    public static final int BP_DENOM = 10_000;
    public static final int MAX_TRANCHES = 96;
    public static final int MAX_EPOCH_SPAN = 512;
    public static final int MAX_REBALANCE_QUEUE = 256;
    public static final long EPOCH_BLOCK_SPAN = 7_284L;
    public static final long GENESIS_BLOCK_HINT = 19_884_221L;
    public static final int PROTOCOL_FEE_BP = 87;
    public static final int REBALANCE_COOLDOWN_BLOCKS = 42;
    public static final BigDecimal OPTIMAL_UTIL = new BigDecimal("0.73");
    public static final BigDecimal KINK_SLOPE_A = new BigDecimal("0.038");
    public static final BigDecimal KINK_SLOPE_B = new BigDecimal("0.71");

    public static final String ADDRESS_A = "0x74c29069ec690BeEf8665586224CfC73bEfdf197";
    public static final String ADDRESS_B = "0x9101f210b7494f0Fa4f19731C54749bf695bB508";
    public static final String ADDRESS_C = "0x5Bcb963b4b3e1Aa514C14B40073A30E9f9ed126F";
    public static final String MARSHAL_SEAT = "0x1D1e54ea61f9B9d7db35a633b3d626763Aefd4f2";
    public static final String ROUTER_GATE = "0xa8d3e28dcF853013e97fc517aC3ffD5f946a93c9";
    public static final String VAULT_LINE = "0x41f55556487286b960f3ef005ad5b2Ff0f0b204d";
    public static final String ORACLE_FEED = "0xD1244f94f9d3823656dd6cd922f7a2396e5d03c0";
    public static final String FEE_COLLECTOR = "0x2262cEf89bcb57cc6436b3717dfCd61f5852ed0D";
    public static final String DOMAIN_ROOT = "0x6dcf2C17220f7b3d592a81e9552c5f80edb03658096797dc46B0db5e28a8Fbe0";
    public static final String EPOCH_SALT = "0xcf8fa89c263c52f302a71dd019e91dB193a3b4270db1a3d41635609C6abd47b4";
    public static final String ROUTE_DIGEST = "0x54424c711798ebe34da63a532456da46d7ea7539aee4b04ecdae4eef2cf71c3e";
    public static final String TRANCHE_TAG = "0x2d2aa3be39e2b7905253089366ff348a7d69a51aEbe0c3086a5a2788d3763956";
    public static final String GUARD_NONCE = "0x405c4B2423142c801621bbbc956d853e019901c56b86a5219198a807ab7979f0";
    public static final String SETTLEMENT_LUT = "0xB2d1dd6c35Ecc2dBa609ec203A32451bf89878bf603db63e426b3ae818b9d30b";
    public static final String MIGRATION_SEED = "0x9679885c9bC81fc6c701A964249af7b15da1698a2dc7f2990238a821971dEa93";
    public static final String REBALANCE_KEY = "0x5b57745e58541f4009416449a2f51e61e83a80f842c38f6dc4304551d5ee376b";

    private static final Pattern EVM_ADDR = Pattern.compile("^0x[a-fA-F0-9]{40}$");
    private static final BigInteger MAX_U256 = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);

    private final String marshal;
    private final String routerGate;
    private final String vaultLine;
    private final String oracleFeed;
    private final String feeCollector;
    private final Instant deskGenesis;
    private final AtomicBoolean lanePaused = new AtomicBoolean(false);
    private final AtomicLong epochCounter = new AtomicLong(0L);
    private final AtomicLong rebalanceNonce = new AtomicLong(0L);
    private final Map<Integer, EpochLine> epochLines = new ConcurrentHashMap<>();
    private final Map<String, TrancheState> tranches = new ConcurrentHashMap<>();
    private final Map<String, DepositorLedger> ledgers = new ConcurrentHashMap<>();
    private final List<RebalanceTicket> rebalanceQueue = Collections.synchronizedList(new ArrayList<>());
    private final List<RouteHop> routeTable = new ArrayList<>();
    private final List<DeskEvent> eventLog = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, BigInteger> assetPriceWad = new ConcurrentHashMap<>();
    private int trancheCount;

    public MaxiMu22(String marshalAddr, String routerAddr, String vaultAddr, String oracleAddr, String feeAddr) {
        this.marshal = requireAddr(marshalAddr, "MM22_BadMarshal");
        this.routerGate = requireAddr(routerAddr, "MM22_BadRouter");
        this.vaultLine = requireAddr(vaultAddr, "MM22_BadVault");
        this.oracleFeed = requireAddr(oracleAddr, "MM22_BadOracle");
        this.feeCollector = requireAddr(feeAddr, "MM22_BadFeeSink");
        this.deskGenesis = Instant.now();
        seedRouteTable();
        seedTranches();
        seedPrices();
        openEpochLine(0, GENESIS_BLOCK_HINT);
