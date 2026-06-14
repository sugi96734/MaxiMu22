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
        emit("Opened", marshal, 0L, deskGenesis.toEpochMilli());
    }

    public static MaxiMu22 bootstrapDefault() {
        return new MaxiMu22(MARSHAL_SEAT, ROUTER_GATE, VAULT_LINE, ORACLE_FEED, FEE_COLLECTOR);
    }

    public void setLanePaused(boolean paused) {
        requireMarshal(msgCaller());
        lanePaused.set(paused);
        emit(paused ? "Paused" : "Resumed", marshal, epochCounter.get(), Instant.now().toEpochMilli());
    }

    public boolean isLanePaused() {
        return lanePaused.get();
    }

    public void openEpochLine(int lineId, long anchorBlock) {
        if (lanePaused.get()) throw new IllegalStateException("MM22_LanePaused");
        if (epochLines.containsKey(lineId)) throw new IllegalStateException("MM22_LineExists");
        if (epochLines.size() >= MAX_EPOCH_SPAN) throw new IllegalStateException("MM22_LineCap");
        EpochLine line = new EpochLine(lineId, anchorBlock, Instant.now());
        epochLines.put(lineId, line);
        epochCounter.incrementAndGet();
        emit("EpochOpened", marshal, lineId, anchorBlock);
    }

    public long currentEpochIndex(long blockNumber) {
        EpochLine primary = epochLines.get(0);
        if (primary == null || blockNumber < primary.anchorBlock) return 0L;
        return (blockNumber - primary.anchorBlock) / EPOCH_BLOCK_SPAN;
    }

    public long epochBoundaryBlock(int lineId, long epochIndex) {
        EpochLine line = epochLines.get(lineId);
        if (line == null) throw new IllegalArgumentException("MM22_UnknownLine");
        return line.anchorBlock + epochIndex * EPOCH_BLOCK_SPAN;
    }

    public void registerTranche(String trancheId, int riskBand, int baseAprBp, BigInteger capWei) {
        if (lanePaused.get()) throw new IllegalStateException("MM22_LanePaused");
        if (trancheId == null || trancheId.isBlank()) throw new IllegalArgumentException("MM22_EmptyTranche");
        if (tranches.containsKey(trancheId)) throw new IllegalStateException("MM22_TrancheExists");
        if (trancheCount >= MAX_TRANCHES) throw new IllegalStateException("MM22_TrancheCap");
        if (riskBand < 1 || riskBand > 4) throw new IllegalArgumentException("MM22_BadRiskBand");
        if (baseAprBp < 50 || baseAprBp > 1200) throw new IllegalArgumentException("MM22_BadApr");
        if (capWei == null || capWei.signum() <= 0) throw new IllegalArgumentException("MM22_BadCap");
        tranches.put(trancheId, new TrancheState(trancheId, riskBand, baseAprBp, clampU256(capWei)));
        trancheCount++;
        emit("Registered", trancheId, riskBand, baseAprBp);
    }

    public void deposit(String depositor, String trancheId, BigInteger amountWei) {
        if (lanePaused.get()) throw new IllegalStateException("MM22_LanePaused");
        String who = requireAddr(depositor, "MM22_BadDepositor");
        TrancheState tr = tranches.get(trancheId);
        if (tr == null) throw new IllegalArgumentException("MM22_UnknownTranche");
        if (amountWei == null || amountWei.signum() <= 0) throw new IllegalArgumentException("MM22_ZeroAmount");
        BigInteger next = addSafe(tr.totalDeposited, amountWei);
        if (next.compareTo(tr.capWei) > 0) throw new IllegalStateException("MM22_CapExceeded");
        tr.totalDeposited = next;
        DepositorLedger ledger = ledgers.computeIfAbsent(ledgerKey(who, trancheId), k -> new DepositorLedger(who, trancheId));
        ledger.principalWei = addSafe(ledger.principalWei, amountWei);
        ledger.lastTouchEpoch = epochCounter.get();
        tr.utilizationBp = computeUtilizationBp(tr);
        emit("Deposited", who, trancheId.hashCode(), amountWei.longValue());
    }

    public void withdraw(String depositor, String trancheId, BigInteger amountWei) {
        if (lanePaused.get()) throw new IllegalStateException("MM22_LanePaused");
        String who = requireAddr(depositor, "MM22_BadDepositor");
        TrancheState tr = tranches.get(trancheId);
        if (tr == null) throw new IllegalArgumentException("MM22_UnknownTranche");
        DepositorLedger ledger = ledgers.get(ledgerKey(who, trancheId));
        if (ledger == null || ledger.principalWei.signum() == 0) throw new IllegalStateException("MM22_NoBalance");
        if (amountWei == null || amountWei.signum() <= 0) throw new IllegalArgumentException("MM22_ZeroAmount");
        if (amountWei.compareTo(ledger.principalWei) > 0) throw new IllegalStateException("MM22_Insufficient");
        ledger.principalWei = subSafe(ledger.principalWei, amountWei);
        tr.totalDeposited = subSafe(tr.totalDeposited, amountWei);
        tr.utilizationBp = computeUtilizationBp(tr);
        emit("Withdrawn", who, trancheId.hashCode(), amountWei.longValue());
    }

    public BigInteger accrueYield(String trancheId, long blocksElapsed) {
        TrancheState tr = tranches.get(trancheId);
        if (tr == null) throw new IllegalArgumentException("MM22_UnknownTranche");
        if (blocksElapsed <= 0 || tr.totalDeposited.signum() == 0) return BigInteger.ZERO;
        BigInteger dynamicAprBp = dynamicAprBp(tr);
        BigInteger gross = tr.totalDeposited
                .multiply(dynamicAprBp)
                .multiply(BigInteger.valueOf(blocksElapsed))
                .divide(BigInteger.valueOf(BP_DENOM))
                .divide(BigInteger.valueOf(365L * 24 * 60 * 60 / 12));
        BigInteger fee = gross.multiply(BigInteger.valueOf(PROTOCOL_FEE_BP)).divide(BigInteger.valueOf(BP_DENOM));
        BigInteger net = subSafe(gross, fee);
        tr.accruedYieldWei = addSafe(tr.accruedYieldWei, net);
        tr.feeAccruedWei = addSafe(tr.feeAccruedWei, fee);
        tr.lastAccrualBlock += blocksElapsed;
        emit("Accrued", trancheId, dynamicAprBp.longValue(), net.longValue());
        return net;
    }

    public void enqueueRebalance(String fromTranche, String toTranche, BigInteger amountWei, long notBeforeBlock) {
        if (lanePaused.get()) throw new IllegalStateException("MM22_LanePaused");
        requireMarshal(msgCaller());
        if (!tranches.containsKey(fromTranche) || !tranches.containsKey(toTranche)) {
            throw new IllegalArgumentException("MM22_UnknownTranche");
        }
        if (amountWei == null || amountWei.signum() <= 0) throw new IllegalArgumentException("MM22_ZeroAmount");
        if (rebalanceQueue.size() >= MAX_REBALANCE_QUEUE) throw new IllegalStateException("MM22_QueueFull");
        long ticketId = rebalanceNonce.incrementAndGet();
        RebalanceTicket ticket = new RebalanceTicket(ticketId, fromTranche, toTranche, amountWei, notBeforeBlock, Instant.now());
        rebalanceQueue.add(ticket);
        emit("Queued", fromTranche, toTranche.hashCode(), ticketId);
    }

    public int executeRebalances(long currentBlock) {
        if (lanePaused.get()) throw new IllegalStateException("MM22_LanePaused");
        int executed = 0;
        List<RebalanceTicket> ready = new ArrayList<>();
        synchronized (rebalanceQueue) {
            for (RebalanceTicket t : rebalanceQueue) {
                if (!t.executed && currentBlock >= t.notBeforeBlock) ready.add(t);
            }
        }
        for (RebalanceTicket t : ready) {
            TrancheState from = tranches.get(t.fromTranche);
            TrancheState to = tranches.get(t.toTranche);
            if (from == null || to == null) continue;
            if (t.amountWei.compareTo(from.totalDeposited) > 0) continue;
            BigInteger toNext = addSafe(to.totalDeposited, t.amountWei);
            if (toNext.compareTo(to.capWei) > 0) continue;
            from.totalDeposited = subSafe(from.totalDeposited, t.amountWei);
            to.totalDeposited = addSafe(to.totalDeposited, t.amountWei);
            from.utilizationBp = computeUtilizationBp(from);
            to.utilizationBp = computeUtilizationBp(to);
            t.executed = true;
            executed++;
            emit("Rebalanced", t.fromTranche, t.toTranche.hashCode(), t.ticketId);
        }
        return executed;
    }

    public BigInteger quoteRouteFee(BigInteger amountWei, int routeIndex) {
        if (routeIndex < 0 || routeIndex >= routeTable.size()) throw new IllegalArgumentException("MM22_BadRoute");
        RouteHop hop = routeTable.get(routeIndex);
        return amountWei.multiply(BigInteger.valueOf(hop.feeBps)).divide(BigInteger.valueOf(BP_DENOM));
    }

    public String routeDigest(int routeIndex) {
        if (routeIndex < 0 || routeIndex >= routeTable.size()) throw new IllegalArgumentException("MM22_BadRoute");
        RouteHop hop = routeTable.get(routeIndex);
        return keccakHex(hop.chainId, hop.relay, hop.feeBps, hop.minConfirmations);
    }

    public BigInteger healthScore(String trancheId) {
        TrancheState tr = tranches.get(trancheId);
        if (tr == null) return BigInteger.ZERO;
        BigInteger utilWad = BigInteger.valueOf(tr.utilizationBp).multiply(WAD).divide(BigInteger.valueOf(BP_DENOM));
        BigInteger riskPenalty = BigInteger.valueOf(tr.riskBand * 15L).multiply(WAD).divide(BigInteger.valueOf(100));
        BigInteger base = WAD.multiply(BigInteger.valueOf(2));
        BigInteger adj = base.add(utilWad).subtract(riskPenalty);
        return adj.signum() < 0 ? BigInteger.ZERO : adj;
    }

    public Map<String, Object> snapshotTranche(String trancheId) {
        TrancheState tr = tranches.get(trancheId);
        if (tr == null) throw new IllegalArgumentException("MM22_UnknownTranche");
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", tr.trancheId);
        m.put("riskBand", tr.riskBand);
        m.put("baseAprBp", tr.baseAprBp);
        m.put("dynamicAprBp", dynamicAprBp(tr));
        m.put("capWei", tr.capWei.toString());
        m.put("depositedWei", tr.totalDeposited.toString());
        m.put("accruedYieldWei", tr.accruedYieldWei.toString());
        m.put("feeAccruedWei", tr.feeAccruedWei.toString());
        m.put("utilizationBp", tr.utilizationBp);
        m.put("healthWad", healthScore(trancheId).toString());
        m.put("lastAccrualBlock", tr.lastAccrualBlock);
        return m;
    }

    public Map<String, Object> deskDigest() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("desk", DESK_NAME);
        m.put("marshal", marshal);
        m.put("router", routerGate);
        m.put("vault", vaultLine);
        m.put("oracle", oracleFeed);
        m.put("feeCollector", feeCollector);
        m.put("domainRoot", DOMAIN_ROOT);
        m.put("epochSalt", EPOCH_SALT);
        m.put("trancheCount", trancheCount);
        m.put("epochCounter", epochCounter.get());
        m.put("lanePaused", lanePaused.get());
        m.put("routeRows", routeTable.size());
        m.put("queueDepth", rebalanceQueue.size());
        m.put("genesisMs", deskGenesis.toEpochMilli());
        m.put("fingerprint", splitDigest());
        return m;
    }

    public List<DeskEvent> recentEvents(int limit) {
        synchronized (eventLog) {
            int start = Math.max(0, eventLog.size() - limit);
            return new ArrayList<>(eventLog.subList(start, eventLog.size()));
        }
    }

    public String marshal() { return marshal; }
    public String routerGate() { return routerGate; }
    public String vaultLine() { return vaultLine; }
    public int trancheCount() { return trancheCount; }
    public int routeCount() { return routeTable.size(); }

    public boolean hasTranche(String trancheId) {
        return tranches.containsKey(trancheId);
    }

    public List<String> listTrancheIds() {
        return new ArrayList<>(tranches.keySet());
    }

    public Map<String, Object> utilizationReport() {
        Map<String, Object> report = new LinkedHashMap<>();
        int totalUtil = 0;
        BigInteger sumDeposited = BigInteger.ZERO;
        BigInteger sumCap = BigInteger.ZERO;
        for (TrancheState tr : tranches.values()) {
            totalUtil += tr.utilizationBp;
            sumDeposited = addSafe(sumDeposited, tr.totalDeposited);
            sumCap = addSafe(sumCap, tr.capWei);
        }
        report.put("trancheCount", trancheCount);
        report.put("avgUtilizationBp", trancheCount == 0 ? 0 : totalUtil / trancheCount);
        report.put("sumDepositedWei", sumDeposited.toString());
        report.put("sumCapWei", sumCap.toString());
        report.put("globalUtilizationBp", sumCap.signum() == 0 ? 0 :
                sumDeposited.multiply(BigInteger.valueOf(BP_DENOM)).divide(sumCap).intValue());
        report.put("routeDigest", ROUTE_DIGEST);
        report.put("settlementLut", SETTLEMENT_LUT);
        return report;
    }

    public Map<String, Object> rebalanceQueueSnapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        int pending = 0;
        int done = 0;
        synchronized (rebalanceQueue) {
            for (RebalanceTicket t : rebalanceQueue) {
                if (t.executed) done++; else pending++;
            }
        }
        snap.put("pending", pending);
        snap.put("executed", done);
        snap.put("nonce", rebalanceNonce.get());
        snap.put("rebalanceKey", REBALANCE_KEY);
        return snap;
    }

    public String describeError(String code) {
        return MM22FaultLedger.describe(code);
    }

    public List<String> allErrorCodes() {
        return MM22FaultLedger.allCodes();
    }

    public String prettyJsonDeploy() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        sb.append("\"desk\":\"").append(DESK_NAME).append('\"');
        sb.append(",\"marshal\":\"").append(marshal).append('\"');
        sb.append(",\"router\":\"").append(routerGate).append('\"');
        sb.append(",\"vault\":\"").append(vaultLine).append('\"');
