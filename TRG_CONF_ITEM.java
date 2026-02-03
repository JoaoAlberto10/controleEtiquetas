import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.PlatformService;
import br.com.sankhya.modelcore.PlatformServiceFactory;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class TRG_CONF_ITEM implements EventoProgramavelJava {

    @Override public void afterDelete(PersistenceEvent event) throws Exception {}
    @Override public void beforeCommit(TransactionContext tranCtx) throws Exception {}
    @Override public void beforeDelete(PersistenceEvent event) throws Exception {
        // validarConferencia(event);

        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();

        BigDecimal nuConf = vo.asBigDecimal("NUCONF");
        BigDecimal sequencia = vo.asBigDecimal("SEQUENCIA");
        String codigoBarras = vo.asString("CODBARRAPDV");
        BigDecimal codigoLocal = vo.asBigDecimal("CODLOCALORIG");
        if (nuConf == null)
            throw new MGEModelException("NUCONF não informado.");

        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT C.NUNOTACONF,I.CODPROD,I.SEQUENCIA FROM AD_FTICONFERENCIA C "
                + "INNER JOIN AD_FTICONFERENCIAITEM I ON C.NUCONF = I.NUCONF WHERE C.NUCONF = :NUCONF AND I.SEQUENCIA = :SEQUENCIA");
        ns.setNamedParameter("NUCONF", nuConf);
        ns.setNamedParameter("SEQUENCIA", sequencia);
        BigDecimal nunota = null;
        BigDecimal codigoProduto = null;
        BigDecimal seq = null;
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()){
                nunota = rs.getBigDecimal("NUNOTACONF");
                codigoProduto = rs.getBigDecimal("CODPROD");
                seq = rs.getBigDecimal("SEQUENCIA");
            }
        }
        deleteByCodigo(nunota, codigoProduto,seq);

        NativeSql up2 = new NativeSql(jdbc);
        up2.appendSql("UPDATE AD_FTICODBARRAITEM SET ATIVO='S', CODLOCAL=:LOC WHERE CODBARRA=:CB");
        up2.setNamedParameter("LOC", codigoLocal);
        up2.setNamedParameter("CB", codigoBarras);
        up2.executeUpdate();


    }
    @Override public void afterInsert(PersistenceEvent event) throws Exception {
        incluirOuAtualizarNota(event);
    }
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void beforeUpdate(PersistenceEvent event) throws Exception {}

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        validarConferencia(event);
    }

    public static boolean deleteByCodigo(BigDecimal nunota, BigDecimal codigoProduto, BigDecimal sequencia) throws MGEModelException {
        JapeSession.SessionHandle hnd = null;
        try {
            hnd = JapeSession.open();
            JapeWrapper pedagioDAO = JapeFactory.dao((String)"ItemNota");
            pedagioDAO.deleteByCriteria("NUNOTA = ? AND CODPROD=? AND SEQUENCIA=? ", new Object[]{nunota,codigoProduto,sequencia});
            return true;
        }
        catch (Exception e) {
            MGEModelException.throwMe((Throwable)e);
        }
        finally {
            JapeSession.close((JapeSession.SessionHandle)hnd);
        }
        return false;
    }

    private void validarConferencia(PersistenceEvent event) throws Exception {
        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();

        BigDecimal nuConf = vo.asBigDecimal("NUCONF");
        if (nuConf == null)
            throw new MGEModelException("NUCONF não informado.");

        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT NUNOTACONF, NUNOTAORIG, TOPCONF, CODLOCAL, CODLOCALDEST, TIPMOV FROM AD_FTICONFERENCIA WHERE NUCONF = :NUCONF");
        ns.setNamedParameter("NUCONF", nuConf);

        BigDecimal top = null;
        BigDecimal codLocal = null;
        BigDecimal codLocalDest = null;
        BigDecimal nuNotaOrig = null;
        String tipMov = null;

        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) {
                top = rs.getBigDecimal("TOPCONF");
                codLocal = rs.getBigDecimal("CODLOCAL");
                codLocalDest = rs.getBigDecimal("CODLOCALDEST");
                nuNotaOrig = rs.getBigDecimal("NUNOTAORIG");
                tipMov = rs.getString("TIPMOV");
            } else {
                throw new MGEModelException("Conferência não encontrada.");
            }
        }

        String codBarra = vo.asString("CODBARRAPDV");
        if (codBarra == null || codBarra.trim().isEmpty())
            throw new MGEModelException("Código de barras não informado.");


        BigDecimal nuNotaConf = null;
        NativeSql buscaNotaConf = new NativeSql(jdbc);
        buscaNotaConf.appendSql("SELECT NUNOTACONF FROM AD_FTICONFERENCIA WHERE NUCONF = :NUCONF");
        buscaNotaConf.setNamedParameter("NUCONF", nuConf);
        try (ResultSet rs = buscaNotaConf.executeQuery()) {
            if (rs.next()) nuNotaConf = rs.getBigDecimal("NUNOTACONF");
        }


        if (nuNotaConf != null) {
            NativeSql sqlVerifica = new NativeSql(jdbc);
            sqlVerifica.appendSql(
                    "SELECT 1 " +
                            "  FROM AD_FTICODBARRAITEM ETI " +
                            " WHERE ( (ETI.CODBARRA = :CODBARRA AND EXISTS (SELECT 1 FROM TGFITE I2 WHERE I2.NUNOTA = :NUNOTA AND I2.CODBARRAPDV = ETI.CODBARRAPALETE)) " +
                            "     OR (ETI.CODBARRAPALETE = :CODBARRA AND EXISTS (SELECT 1 FROM TGFITE I3 WHERE I3.NUNOTA = :NUNOTA AND I3.CODBARRAPDV = ETI.CODBARRA)) )"
            );
            sqlVerifica.setNamedParameter("CODBARRA", codBarra);
            sqlVerifica.setNamedParameter("NUNOTA", nuNotaConf);
            try (ResultSet rs = sqlVerifica.executeQuery()) {
                if (rs.next()) {
                    throw new MGEModelException("Não é permitido bipar o palete (" + codBarra + ") e suas caixas na mesma conferência!");
                }
            }
        }



        boolean isPalete = existePaleteAtivoPorCodigo(jdbc, codBarra);
        boolean existeItem = existeItemAtivoPorCodigo(jdbc, codBarra);

        /*VERIFICAR TOP 411 RE-ENTRADA ETIQUETA*/
        NativeSql check411 = new NativeSql(jdbc);
        check411.appendSql(
                "SELECT 1 FROM TGFCAB WHERE CODTIPOPER = 411 AND NUNOTA = :NUNOTA_ORIG"
        );
        check411.setNamedParameter("NUNOTA_ORIG", nuNotaOrig);

        boolean isTop411 = false;
        try (ResultSet rs = check411.executeQuery()) {
            if (rs.next()) isTop411 = true;
        }
        /*FIM*/

        if ((top.intValue() != 1704 && !isTop411) && !isPalete && !existeItem) {
            throw new MGEModelException("Código de barras inexistente ou inativo.");
        }

        if (nuNotaOrig != null) {
            NativeSql check406 = new NativeSql(jdbc);
            check406.appendSql(
                    "SELECT 1 FROM TGFCAB WHERE CODTIPOPER = 406 AND NUNOTA = :NUNOTA_ORIG"
            );
            check406.setNamedParameter("NUNOTA_ORIG", nuNotaOrig);

            boolean isTop406 = false;
            try (ResultSet rs = check406.executeQuery()) {
                if (rs.next()) isTop406 = true;
            }

            if (isTop406) {
                // Pega a nota à qual a etiqueta pertence
                NativeSql getEtiquetaNota = new NativeSql(jdbc);
                getEtiquetaNota.appendSql(
                        "SELECT NUNOTA FROM AD_FTICODBARRAITEM WHERE CODBARRA = :CB"
                );
                getEtiquetaNota.setNamedParameter("CB", codBarra);

                BigDecimal nunotaEtiqueta = null;
                try (ResultSet rs = getEtiquetaNota.executeQuery()) {
                    if (rs.next()) nunotaEtiqueta = rs.getBigDecimal("NUNOTA");
                }

                if (nunotaEtiqueta == null || nunotaEtiqueta.compareTo(nuNotaOrig) != 0) {
                    throw new MGEModelException(
                            "Etiqueta " + codBarra +
                                    " não pertence ao pedido original nº " + nuNotaOrig +
                                    ". Apenas etiquetas dessa nota podem ser conferidas (TOP 406)."
                    );
                }
            }
        }


        BigDecimal codProd = vo.asBigDecimal("CODPROD");
        String codVol = vo.asString("CODVOL");
        BigDecimal qtdConf = vo.asBigDecimal("QTDNEG");
        if (qtdConf == null) qtdConf = BigDecimal.ZERO;

        NativeSql pend = new NativeSql(jdbc);
        pend.appendSql(
                "SELECT DISTINCT " +
                        "(CASE WHEN NVL(ITE.AD_CODVOL, ITE.CODVOL) = PRO.CODVOL THEN ITE.QTDNEG "+
                        "ELSE ITE.QTDNEG / NVL(VOA.QUANTIDADE,1) END) "+
                        "- NVL(SUM(CASE "+
                        "WHEN ITE2.CODVOL = PRO.CODVOL AND ITE2.CODBARRAPDV IS NOT NULL THEN ITE2.QTDNEG "+
                        "WHEN ITE2.CODVOL <> PRO.CODVOL AND ITE2.CODBARRAPDV IS NOT NULL THEN ITE2.QTDNEG / NVL(VOA.QUANTIDADE,1) "+
                        "END) OVER (PARTITION BY ITE.CODPROD, NVL(ITE.AD_CODVOL, ITE.CODVOL)), 0) AS PENDENTE "+
                        "FROM TGFCAB CAB "+
                        "INNER JOIN TGFITE ITE ON ITE.NUNOTA = CAB.NUNOTA "+
                        "INNER JOIN TGFPRO PRO ON PRO.CODPROD = ITE.CODPROD "+
                        "LEFT JOIN TGFVOA VOA ON VOA.CODPROD = ITE.CODPROD AND VOA.CODVOL = NVL(ITE.AD_CODVOL, ITE.CODVOL) "+
                        "INNER JOIN AD_FTICONFERENCIA CON ON CON.NUNOTAORIG = CAB.NUNOTA "+
                        "LEFT JOIN TGFCAB CAB2 ON CAB2.NUNOTA = CON.NUNOTACONF "+
                        "LEFT JOIN TGFITE ITE2 ON ITE2.NUNOTA = CAB2.NUNOTA "+
                        "AND ITE.CODPROD = ITE2.CODPROD "+
                        "AND NVL(ITE.AD_CODVOL, ITE.CODVOL) = ITE2.CODVOL "+
                        "AND ITE2.SEQUENCIA>0 " +
                        "WHERE CON.NUCONF = :NUCONF "+
                        "AND ITE.CODPROD = :CODPROD "+
                        "AND NVL(ITE.AD_CODVOL, ITE.CODVOL) = :CODVOL "+
                        "FETCH FIRST 1 ROW ONLY "
        );
        pend.setNamedParameter("NUCONF", nuConf);
        pend.setNamedParameter("CODPROD", codProd);
        pend.setNamedParameter("CODVOL", codVol);

        BigDecimal pendente = BigDecimal.ZERO;
        try (ResultSet rs = pend.executeQuery()) {
            if (rs.next()) {
                pendente = rs.getBigDecimal("PENDENTE");
                if (pendente == null)
                    pendente = BigDecimal.ZERO;
            }
        }

        if (top.intValue() != 1704 && qtdConf.compareTo(pendente) > 0) {
            throw new MGEModelException(
                    "Quantidade informada (" + qtdConf +
                            ") excede o saldo pendente (" + pendente + ") do item " + codProd + ".");
        }

        validarFIFO(jdbc, nuNotaOrig, codBarra, codProd, codVol, codLocal);


        // ===========================================================
        // 🔹 Continua validações normais de locais, etc.
        // ===========================================================
        if (isPalete) {
            validarPalete(jdbc, top, codBarra, codLocal, codLocalDest, nuConf);
        } else {
            validarEtiqueta(jdbc, top, codBarra, codLocal, codLocalDest, nuConf);
        }
    }


    private void validarPalete(JdbcWrapper jdbc, BigDecimal top, String codBarra,
                               BigDecimal codLocal, BigDecimal codLocalDest, BigDecimal nuConf) throws Exception {
        BigDecimal codLocalPalete = null;
        NativeSql s = new NativeSql(jdbc);

        // ➤ No inventário (1704) não exige que o palete esteja ativo
        if (top.intValue() == 1704) {
            s.appendSql("SELECT CODLOCAL FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB");
        } else {
            s.appendSql("SELECT CODLOCAL FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB AND ATIVO='S'");
        }

        s.setNamedParameter("CB", codBarra);
        try (ResultSet rs = s.executeQuery()) {
            if (rs.next()) {
                codLocalPalete = rs.getBigDecimal(1);
            } else if (top.intValue() != 1704) {
                // Apenas fora do inventário lança erro de inatividade
                throw new MGEModelException("Palete inválido ou inativo.");
            }
        }

        if (top.intValue() == 1701) {
            if (codLocalPalete == null || codLocal == null || codLocalPalete.compareTo(codLocal) != 0)
                throw new MGEModelException("Local do palete não corresponde ao local da conferência (Saída).");
        } else if (top.intValue() == 1702) {
            if (codLocalPalete != null)
                throw new MGEModelException("Palete já possui local definido, deveria estar vazio (Entrada).");
        } else if (top.intValue() == 1703) {
            if (codLocalPalete == null)
                throw new MGEModelException("Palete sem local definido (Transferência).");
            if (codLocalDest == null || codLocalDest.compareTo(codLocalPalete) == 0)
                throw new MGEModelException("Local de destino do palete inválido.");
        } else if (top.intValue() == 1704) {
            NativeSql chk = new NativeSql(jdbc);
            chk.appendSql("SELECT COUNT(*) QT FROM AD_FTICONFERENCIAITEM WHERE CODBARRAPDV = :CB AND NUCONF = :NUCONF");
            chk.setNamedParameter("CB", codBarra);
            chk.setNamedParameter("NUCONF", nuConf);
            try (ResultSet rs = chk.executeQuery()) {
                if (rs.next() && rs.getBigDecimal("QT").compareTo(BigDecimal.ZERO) > 0)
                    throw new MGEModelException("Palete já informado nesta conferência (Inventário).");
            }
        }
    }


    private void validarEtiqueta(JdbcWrapper jdbc, BigDecimal top, String codBarra,
                                 BigDecimal codLocal, BigDecimal codLocalDest, BigDecimal nuConf) throws Exception {
        NativeSql dup = new NativeSql(jdbc);
        dup.appendSql("SELECT COUNT(*) QT FROM AD_FTICONFERENCIAITEM WHERE CODBARRAPDV = :CB AND NUCONF = :P2");
        dup.setNamedParameter("CB", codBarra);
        dup.setNamedParameter("P2", nuConf);
        try (ResultSet rs = dup.executeQuery()) {
            if (rs.next() && rs.getBigDecimal("QT").compareTo(BigDecimal.ZERO) > 0)
                throw new MGEModelException("Etiqueta já conferida nesta conferência.");
        }

        BigDecimal codLocalEtiqueta = null;
        NativeSql loc = new NativeSql(jdbc);

        // ➤ No inventário (1704) pode bipar etiqueta mesmo inativa
        if (top.intValue() == 1704) {
            loc.appendSql("SELECT CODLOCAL FROM AD_FTICODBARRAITEM WHERE CODBARRA = :CB");
        } else {
            loc.appendSql("SELECT CODLOCAL FROM AD_FTICODBARRAITEM WHERE CODBARRA = :CB AND ATIVO='S'");
        }

        loc.setNamedParameter("CB", codBarra);
        try (ResultSet rs = loc.executeQuery()) {
            if (rs.next()) {
                codLocalEtiqueta = rs.getBigDecimal(1);
            } else if (top.intValue() != 1704) {
                BigDecimal TOPorigem = consultarTopNunotaOrigem(jdbc, nuConf);

                if(TOPorigem.compareTo(new BigDecimal(411)) != 0){

                    throw new MGEModelException("Etiqueta inválida ou inativa.");
                }
            }
        }

        if (top.intValue() == 1701) {
            if (codLocalEtiqueta == null || codLocal == null || codLocalEtiqueta.compareTo(codLocal) != 0)
                throw new MGEModelException("Local da etiqueta não corresponde ao local da conferência (Saída).");
        } else if (top.intValue() == 1702) {
            BigDecimal TOPorigem = consultarTopNunotaOrigem(jdbc, nuConf);

            if (codLocalEtiqueta != null && TOPorigem.compareTo(new BigDecimal(411)) != 0)
                throw new MGEModelException("Etiqueta já possui local definido (Entrada).");

        } else if (top.intValue() == 1703) {
            if (codLocalEtiqueta == null)
                throw new MGEModelException("Etiqueta sem local definido (Transferência).");
            if (codLocalDest == null ||
                    (codLocalEtiqueta.compareTo(codLocal) != 0 && codLocalEtiqueta.compareTo(codLocalDest) != 0))
                throw new MGEModelException("Etiqueta não corresponde ao local de origem nem ao de destino (Transferência).");
        } else if (top.intValue() == 1704) {
            NativeSql chk = new NativeSql(jdbc);
            chk.appendSql("SELECT COUNT(*) QT FROM AD_FTICONFERENCIAITEM WHERE CODBARRAPDV = :CB AND NUCONF = :NUCONF");
            chk.setNamedParameter("CB", codBarra);
            chk.setNamedParameter("NUCONF", nuConf);
            try (ResultSet rs = chk.executeQuery()) {
                if (rs.next() && rs.getBigDecimal("QT").compareTo(BigDecimal.ZERO) > 0)
                    throw new MGEModelException("Etiqueta já informada nesta conferência (Inventário).");
            }
        }
    }

    public static BigDecimal consultarTopNunotaOrigem(JdbcWrapper jdbc, BigDecimal nuconf) throws Exception {

        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql(
                "SELECT CAB.CODTIPOPER " +
                        "  FROM AD_FTICONFERENCIA C " +
                        " INNER JOIN TGFCAB CAB ON CAB.NUNOTA = C.NUNOTAORIG " +
                        " WHERE C.NUCONF = :NUCONF"
        );

        ns.setNamedParameter("NUCONF", nuconf);

        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) {
                return rs.getBigDecimal("CODTIPOPER");
            } else {
                throw new MGEModelException("Conferência não encontrada para NUCONF: " + nuconf);
            }
        } catch (SQLException e) {
            throw new MGEModelException("Erro ao consultar TOP origem: " + e.getMessage(), e);
        }
    }



    private void incluirOuAtualizarNota(PersistenceEvent event) throws Exception {
        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();
        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();

        BigDecimal nuConf = vo.asBigDecimal("NUCONF");
        if (nuConf == null)
            throw new MGEModelException("NUCONF não informado.");

        Map<String, Object> conf = obterConferencia(jdbc, nuConf);
        if (conf == null)
            throw new MGEModelException("Conferência não encontrada.");

        BigDecimal nuNotaConf = (BigDecimal) conf.get("NUNOTACONF");
        BigDecimal top = (BigDecimal) conf.get("TOPCONF");

        if (nuNotaConf == null) {
            nuNotaConf = gerarNotaConferencia(dwf, jdbc, nuConf, conf);
        }

        String codBarra = vo.asString("CODBARRAPDV");
        boolean isPalete = existePaleteAtivoPorCodigo(jdbc, codBarra);
        if (isPalete) {
            processarPalete(jdbc, codBarra, top, conf);
        } else {
            processarEtiqueta(jdbc, codBarra, top, conf);
        }

        incluirItemConferencia(dwf, vo, nuNotaConf, jdbc,vo.asBigDecimal("CODPROD"), vo.asString("CODVOL"),vo.asBigDecimal("QTDNEG"), top, conf);
    }

    private void processarPalete(JdbcWrapper jdbc, String codBarra, BigDecimal top, Map<String, Object> conf) throws Exception {
        BigDecimal codLocal = (BigDecimal) conf.get("CODLOCAL");
        BigDecimal codLocalDest = (BigDecimal) conf.get("CODLOCALDEST");

        if (top.intValue() == 1701) {
            NativeSql up = new NativeSql(jdbc);
            up.appendSql("UPDATE AD_FTICODBARRAPALETE SET ATIVO='N', CODLOCAL=:LOC WHERE CODBARRAPALETE=:CB");
            up.setNamedParameter("LOC", codLocal);
            up.setNamedParameter("CB", codBarra);
            up.executeUpdate();

            NativeSql up2 = new NativeSql(jdbc);
            up2.appendSql("UPDATE AD_FTICODBARRAITEM SET ATIVO='N', CODLOCAL=:LOC WHERE CODBARRAPALETE=:CB");
            up2.setNamedParameter("LOC", codLocal);
            up2.setNamedParameter("CB", codBarra);
            up2.executeUpdate();
        }
        else if (top.intValue() == 1702) {
            NativeSql up = new NativeSql(jdbc);
            up.appendSql("UPDATE AD_FTICODBARRAPALETE SET CODLOCAL=:LOC, ATIVO='S' WHERE CODBARRAPALETE=:CB");
            up.setNamedParameter("LOC", codLocal);
            up.setNamedParameter("CB", codBarra);
            up.executeUpdate();
        }
        else if (top.intValue() == 1703) {
            NativeSql up = new NativeSql(jdbc);
            up.appendSql("UPDATE AD_FTICODBARRAPALETE SET CODLOCAL=:LOC WHERE CODBARRAPALETE=:CB");
            up.setNamedParameter("LOC", codLocalDest);
            up.setNamedParameter("CB", codBarra);
            up.executeUpdate();

            NativeSql up2 = new NativeSql(jdbc);
            up2.appendSql("UPDATE AD_FTICODBARRAITEM SET CODLOCAL=:LOC WHERE CODBARRAPALETE=:CB");
            up2.setNamedParameter("LOC", codLocalDest);
            up2.setNamedParameter("CB", codBarra);
            up2.executeUpdate();
        }
    }

    private void processarEtiqueta(JdbcWrapper jdbc, String codBarra, BigDecimal top, Map<String, Object> conf) throws Exception {
        BigDecimal codLocal = (BigDecimal) conf.get("CODLOCAL");
        BigDecimal codLocalDest = (BigDecimal) conf.get("CODLOCALDEST");

        if (top.intValue() == 1701) {
            NativeSql up = new NativeSql(jdbc);
            up.appendSql("UPDATE AD_FTICODBARRAITEM SET ATIVO='N', CODLOCAL=:LOC WHERE CODBARRA=:CB");
            up.setNamedParameter("LOC", codLocal);
            up.setNamedParameter("CB", codBarra);
            up.executeUpdate();

            NativeSql sel = new NativeSql(jdbc);
            sel.appendSql("SELECT CODBARRAPALETE FROM AD_FTICODBARRAITEM WHERE CODBARRA=:CB AND CODBARRAPALETE IS NOT NULL");
            sel.setNamedParameter("CB", codBarra);
            try (ResultSet rs = sel.executeQuery()) {
                if (rs.next()) {
                    String pal = rs.getString(1);
                    NativeSql des = new NativeSql(jdbc);
                    des.appendSql("UPDATE AD_FTICODBARRAPALETE SET ATIVO='N' WHERE CODBARRAPALETE=:PAL");
                    des.setNamedParameter("PAL", pal);
                    des.executeUpdate();
                }
            }
        }
        else if (top.intValue() == 1702) {
            NativeSql up = new NativeSql(jdbc);
            up.appendSql("UPDATE AD_FTICODBARRAITEM SET CODLOCAL=:LOC, ATIVO='S' WHERE CODBARRA=:CB"); //COLOCAR ATIVO ='S' JOAO MARCOS
            up.setNamedParameter("LOC", codLocal);
            up.setNamedParameter("CB", codBarra);
            up.executeUpdate();
        }
        else if (top.intValue() == 1703) {
            NativeSql up = new NativeSql(jdbc);
            up.appendSql("UPDATE AD_FTICODBARRAITEM SET CODLOCAL=:LOC WHERE CODBARRA=:CB");
            up.setNamedParameter("LOC", codLocalDest);
            up.setNamedParameter("CB", codBarra);
            up.executeUpdate();
        }
    }

    private BigDecimal gerarNotaConferencia(EntityFacade dwf, JdbcWrapper jdbc, BigDecimal nuConf, Map conf) throws Exception {
        Calendar cal = Calendar.getInstance();
        BigDecimal top = (BigDecimal) conf.get("TOPCONF");
        int topInt = top != null ? top.intValue() : 0;
        if (cal.get(Calendar.HOUR_OF_DAY) < 6 && (topInt == 1701 || topInt == 1702)) {
            cal.add(Calendar.DAY_OF_MONTH, -1);
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        DynamicVO cabVO = (DynamicVO) dwf.getDefaultValueObjectInstance(DynamicEntityNames.CABECALHO_NOTA);
        cabVO.setProperty("CODEMP", BigDecimal.ONE);
        cabVO.setProperty("DTNEG", new Timestamp(cal.getTimeInMillis()));
        cabVO.setProperty("DTENTSAI", new Timestamp(cal.getTimeInMillis()));
        cabVO.setProperty("CODPARC", new BigDecimal(999996));
        cabVO.setProperty("CODNAT", new BigDecimal(101001));
        cabVO.setProperty("NUMNOTA", BigDecimal.ZERO);
        cabVO.setProperty("CODTIPOPER", conf.get("TOPCONF"));

        dwf.createEntity(DynamicEntityNames.CABECALHO_NOTA, (EntityVO) cabVO);
        BigDecimal nuNota = cabVO.asBigDecimal("NUNOTA");

        NativeSql up = new NativeSql(jdbc);
        up.appendSql("UPDATE AD_FTICONFERENCIA SET NUNOTACONF=:N WHERE NUCONF=:C");
        up.setNamedParameter("N", nuNota);
        up.setNamedParameter("C", nuConf);
        up.executeUpdate();

        return nuNota;
    }

    private void incluirItemConferencia(EntityFacade dwf, DynamicVO vo, BigDecimal nuNota,
                                        JdbcWrapper jdbc, BigDecimal codProd, String codVol,
                                        BigDecimal quantidade, BigDecimal topConf, Map cabConf) throws Exception {

        BigDecimal seqVol = vo.asBigDecimal("SEQVOL");
        if (seqVol != null) {
            String codBarraPalete = codProd.toPlainString() + codVol + "-" + nuNota.toPlainString() + "-" +
                    String.format("%03d", seqVol.intValue());

            NativeSql updItem = new NativeSql(jdbc);
            updItem.appendSql("UPDATE AD_FTICODBARRAITEM SET CODBARRAPALETE = :PAL WHERE CODBARRA = :CB");
            updItem.setNamedParameter("PAL", codBarraPalete);
            updItem.setNamedParameter("CB", vo.asString("CODBARRAPDV"));
            updItem.executeUpdate();
        }

        gerarPaleteAutomatico(jdbc, vo, (BigDecimal) cabConf.get("NUNOTACONF"));

        // Busca fator de conversão da TGFVOA
        NativeSql top = new NativeSql(jdbc);
        top.appendSql("SELECT CASE WHEN ATUALEST='E' THEN 1  WHEN ATUALEST='B' THEN -1 ELSE 0 END AS ATUALEST FROM TGFTOP WHERE CODTIPOPER = :P1 ORDER BY DHALTER DESC FETCH FIRST 1 ROW ONLY");
        top.setNamedParameter("P1", topConf);

        BigDecimal atualEstoque = BigDecimal.ONE;
        try (ResultSet tops = top.executeQuery()) {
            if (tops.next()) {
                atualEstoque = tops.getBigDecimal(1);
                if (atualEstoque == null) atualEstoque = BigDecimal.ONE;
            }
        }

        // Busca fator de conversão da TGFVOA
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT NVL(QUANTIDADE,0) FROM TGFVOA WHERE CODPROD=:P1 AND CODVOL=:P2");
        ns.setNamedParameter("P1", codProd);
        ns.setNamedParameter("P2", codVol);

        BigDecimal fatorConversao = BigDecimal.ONE;
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) {
                fatorConversao = rs.getBigDecimal(1);
                if (fatorConversao == null) fatorConversao = BigDecimal.ONE;
            }
        }

        BigDecimal qtdEquivalente = quantidade.multiply(fatorConversao);

        // Cria item na nota
        DynamicVO item = (DynamicVO) dwf.getDefaultValueObjectInstance(DynamicEntityNames.ITEM_NOTA);
        item.setProperty("NUNOTA", nuNota);
        item.setProperty("SEQUENCIA", vo.asBigDecimal("SEQUENCIA"));
        item.setProperty("CODPROD", codProd);
        item.setProperty("CODVOL", codVol);
        item.setProperty("CODBARRAPDV", vo.asString("CODBARRAPDV"));
        item.setProperty("ATUALESTOQUE", atualEstoque);
        item.setProperty("QTDNEG", qtdEquivalente);
        if (vo.asBigDecimal("SEQVOL") != null)
            item.setProperty("AD_SEQVOL", vo.asBigDecimal("SEQVOL"));
        if (cabConf.get("CODLOCALDEST") == null)
            item.setProperty("CODLOCALORIG", vo.asBigDecimal("CODLOCALORIG"));
        if (cabConf.get("CODLOCALDEST") != null)
            item.setProperty("CODLOCALORIG", cabConf.get("CODLOCALDEST"));
        if (cabConf.get("CODLOCALDEST") != null)
            item.setProperty("CODLOCALDEST", vo.asBigDecimal("CODLOCALORIG"));

        dwf.createEntity(DynamicEntityNames.ITEM_NOTA, (EntityVO) item);
    }


    private Map<String, Object> obterConferencia(JdbcWrapper jdbc, BigDecimal nuConf) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT * FROM AD_FTICONFERENCIA WHERE NUCONF=:NUCONF");
        ns.setNamedParameter("NUCONF", nuConf);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) {
                Map<String, Object> m = new HashMap<>();
                m.put("NUNOTACONF", rs.getBigDecimal("NUNOTACONF"));
                m.put("TOPCONF", rs.getBigDecimal("TOPCONF"));
                m.put("CODLOCAL", rs.getBigDecimal("CODLOCAL"));
                m.put("CODLOCALDEST", rs.getBigDecimal("CODLOCALDEST"));
                return m;
            }
        }
        return null;
    }

    private BigDecimal obterNUCONFporPalete(JdbcWrapper jdbc, String codBarra) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT NUCONF FROM AD_FTICONFERENCIAITEM WHERE CODBARRAPDV = :CB");
        ns.setNamedParameter("CB", codBarra);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("NUCONF");
        }
        return null;
    }

    private boolean existePaleteAtivoPorCodigo(JdbcWrapper jdbc, String cb) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT 1 FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB AND ATIVO='S'");
        ns.setNamedParameter("CB", cb);
        try (ResultSet rs = ns.executeQuery()) {
            return rs.next();
        }
    }

    private boolean existeItemAtivoPorCodigo(JdbcWrapper jdbc, String cb) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT 1 FROM AD_FTICODBARRAITEM WHERE CODBARRA = :CB AND ATIVO='S'");
        ns.setNamedParameter("CB", cb);
        try (ResultSet rs = ns.executeQuery()) {
            return rs.next();
        }
    }

    private String obterImpressora(JdbcWrapper jdbc, BigDecimal codLocal) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT IMPRESSORA FROM AD_FTICONFERENCIAIMPRES WHERE CODLOCAL=:LOC");
        sql.setNamedParameter("LOC", codLocal);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getString("IMPRESSORA");
        }
        return "?";
    }

    private Integer obterQtdPorPalete(JdbcWrapper jdbc, BigDecimal codProd, String codVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT AD_QTDPALETE FROM TGFVOA WHERE CODPROD=:P1 AND CODVOL=:P2");
        ns.setNamedParameter("P1", codProd);
        ns.setNamedParameter("P2", codVol);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) {
                BigDecimal bd = rs.getBigDecimal(1);
                if (bd != null) return bd.intValue();
            }
        }
        return 0;
    }

    private void imprimirPalete(JdbcWrapper jdbc, String codBarraPalete, BigDecimal codLocal) throws Exception {
        String impressora = obterImpressora(jdbc, codLocal);
        PlatformService ps = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
        ps.set("printer.name", impressora);
        ps.set("nurfe", new BigDecimal(286));
        ps.set("codemp", BigDecimal.ONE);
        Map<String, Object> p = new HashMap<>();
        p.put("P_CODBARRA", codBarraPalete);
        ps.set("report.params", p);
        ps.execute();
    }

    private void gerarPaleteAutomatico(JdbcWrapper jdbc, DynamicVO vo, BigDecimal nuNotaConf) throws Exception {
        BigDecimal codProd = vo.asBigDecimal("CODPROD");
        String codVol = vo.asString("CODVOL");
        BigDecimal seqVol = vo.asBigDecimal("SEQVOL");
        BigDecimal codUsu = vo.asBigDecimal("CODUSUINC");
        BigDecimal codLocal = vo.asBigDecimal("CODLOCALORIG");

        if (seqVol == null) return;

        // Busca quantidade padrão de itens por palete
        Integer qtdPorPalete = obterQtdPorPalete(jdbc, codProd, codVol);
        if (qtdPorPalete == null || qtdPorPalete <= 0) return;

        // 1️⃣ Conta quantas unidades já conferidas desse produto/volume na nota de conferência
        NativeSql sqlQtdConf = new NativeSql(jdbc);
        sqlQtdConf.appendSql(
                "SELECT NVL(SUM( " +
                        "CASE WHEN I.AD_CODVOL = P.CODVOL THEN I.QTDNEG " +
                        "ELSE I.QTDNEG / NVL(V.QUANTIDADE, 1) END " +
                        "), 0) AS QTDCONF " +
                        "FROM TGFITE I " +
                        "JOIN TGFPRO P ON P.CODPROD = I.CODPROD " +
                        "LEFT JOIN TGFVOA V ON V.CODPROD = I.CODPROD AND V.CODVOL = NVL(I.AD_CODVOL, I.CODVOL) " +
                        "WHERE I.NUNOTA = :NUNOTA " +
                        "AND I.CODPROD = :CODPROD " +
                        "AND NVL(I.AD_CODVOL, I.CODVOL) = :CODVOL "
        );
        sqlQtdConf.setNamedParameter("NUNOTA", nuNotaConf);
        sqlQtdConf.setNamedParameter("CODPROD", codProd);
        sqlQtdConf.setNamedParameter("CODVOL", codVol);

        BigDecimal qtdConferida = BigDecimal.ZERO;
        try (ResultSet rs = sqlQtdConf.executeQuery()) {
            if (rs.next()) qtdConferida = rs.getBigDecimal("QTDCONF");
            if (qtdConferida == null) qtdConferida = BigDecimal.ZERO;
        }

        // 2️⃣ Soma a quantidade atual para considerar o bip recém-feito
        BigDecimal qtdAtual = vo.asBigDecimal("QTDNEG");
        if (qtdAtual == null) qtdAtual = BigDecimal.ZERO;

        BigDecimal qtdTotalApos = qtdConferida.add(qtdAtual);
        BigDecimal qtdPalete = new BigDecimal(qtdPorPalete);
        BigDecimal resto = qtdTotalApos.remainder(qtdPalete);

        // 3️⃣ Só gera o palete quando atingir múltiplo exato da QTD por palete
        if (resto.compareTo(BigDecimal.ZERO) == 0 && qtdTotalApos.compareTo(BigDecimal.ZERO) > 0) {

            // Gera código de barras único do palete
            String codBarraPalete = codProd.toPlainString() + codVol + "-" + nuNotaConf.toPlainString() + "-" + String.format("%03d", seqVol.intValue());

            // Evita duplicar o palete
            NativeSql chk = new NativeSql(jdbc);
            chk.appendSql("SELECT 1 FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CB");
            chk.setNamedParameter("CB", codBarraPalete);
            try (ResultSet rs = chk.executeQuery()) {
                if (rs.next()) return; // já existe
            }

            // 4️⃣ Insere o novo palete
            NativeSql ins = new NativeSql(jdbc);
            ins.appendSql(
                    "INSERT INTO AD_FTICODBARRAPALETE " +
                            "(NROCONFIG, CODBARRAPALETE, CODPROD, CODVOL, QTD, DHINC, CODUSU, CODLOCAL, NUNOTA, ATIVO) " +
                            "VALUES (1, :CB, :CP, :CV, :QTD, SYSDATE, :CU, :CL, :N, 'S')"
            );
            ins.setNamedParameter("CB", codBarraPalete);
            ins.setNamedParameter("CP", codProd);
            ins.setNamedParameter("CV", codVol);
            ins.setNamedParameter("QTD", qtdPorPalete);
            ins.setNamedParameter("CU", codUsu);
            ins.setNamedParameter("CL", codLocal);
            ins.setNamedParameter("N", nuNotaConf);
            ins.executeUpdate();

            // 5️⃣ Impressão automática do palete
            String impressora = obterImpressora(jdbc, codLocal);
            PlatformService ps = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
            ps.set("printer.name", impressora);
            ps.set("nurfe", new BigDecimal(286)); // número do relatório da etiqueta de palete
            ps.set("codemp", BigDecimal.ONE);
            Map<String, Object> params = new HashMap<>();
            params.put("P_CODBARRA", codBarraPalete);
            ps.set("report.params", params);
            ps.execute();
        }
    }

    private void validarFIFO(JdbcWrapper jdbc,
                             BigDecimal nuNotaOrig,
                             String codBarra,
                             BigDecimal codProd,
                             String codVol,
                             BigDecimal codLocal) throws Exception {

    /* ===========================================================
       1️⃣ FIFO APENAS PARA PEDIDO DE VENDA
       =========================================================== */
        NativeSql chkPV = new NativeSql(jdbc);
        chkPV.appendSql(
                "SELECT 1 " +
                        "  FROM AD_FTICONFERENCIA C " +
                        "  JOIN TGFCAB CAB ON CAB.NUNOTA = C.NUNOTAORIG " +
                        " WHERE C.NUNOTAORIG = :NUCONF " +
                        "   AND CAB.TIPMOV = 'P'"
        );
        chkPV.setNamedParameter("NUCONF", nuNotaOrig);

        try (ResultSet rs = chkPV.executeQuery()) {
            if (!rs.next()) return;
        }

    /* ===========================================================
       2️⃣ CONSULTA LOG DE LIBERAÇÃO (SEM CODLOCAL)
       =========================================================== */
        NativeSql log = new NativeSql(jdbc);
        log.appendSql(
                "SELECT TENTATIVAS, LIBERACAO " +
                        "  FROM AD_FTILOGLIBFIFOBIP " +
                        " WHERE NUNOTA  = :NUNOTA " +
                        "   AND CODPROD = :CODPROD"
        );
        log.setNamedParameter("NUNOTA", nuNotaOrig);
        log.setNamedParameter("CODPROD", codProd);

        int tentativas = 0;
        String liberacao = null;

        try (ResultSet rs = log.executeQuery()) {
            if (rs.next()) {
                tentativas = rs.getInt("TENTATIVAS");
                liberacao  = rs.getString("LIBERACAO");
            }
        }

        if ("A".equals(liberacao)) return;

        if ("R".equals(liberacao)) {
            throw new MGEModelException(
                    "FIFO bloqueado para este produto. Liberação REPROVADA."
            );
        }

        if ("P".equals(liberacao)) {
            throw new MGEModelException(
                    "FIFO pendente de liberação para este produto. Aguardando aprovação."
            );
        }

    /* ===========================================================
       3️⃣ DATA DA ETIQUETA BIPADA (ESTOQUE REAL)
       =========================================================== */
        NativeSql dtEtiqueta = new NativeSql(jdbc);
        dtEtiqueta.appendSql(
                "SELECT DTPROD " +
                        "  FROM AD_FTICODBARRAITEM " +
                        " WHERE (CODBARRA = :CB OR CODBARRAPALETE = :CB) " +
                        "   AND CODPROD  = :CP " +
                        "   AND CODVOL   = :CV " +
                        "   AND CODLOCAL = :CL"
        );
        dtEtiqueta.setNamedParameter("CB", codBarra);
        dtEtiqueta.setNamedParameter("CP", codProd);
        dtEtiqueta.setNamedParameter("CV", codVol);
        dtEtiqueta.setNamedParameter("CL", codLocal);

        java.sql.Date dtProd;
        try (ResultSet rs = dtEtiqueta.executeQuery()) {
            if (!rs.next())
                throw new MGEModelException("Etiqueta não encontrada para validação FIFO.");
            dtProd = rs.getDate("DTPROD");
        }

    /* ===========================================================
       4️⃣ FIFO MAIS ANTIGO ATIVO (ESTOQUE)
       =========================================================== */
        NativeSql fifo = new NativeSql(jdbc);
        fifo.appendSql(
                "SELECT MIN(DTPROD) AS DTPROD_OLD " +
                        "  FROM AD_FTICODBARRAITEM " +
                        " WHERE ATIVO='S' " +
                        "   AND CODPROD  = :CP " +
                        "   AND CODVOL   = :CV " +
                        "   AND CODLOCAL = :CL"
        );
        fifo.setNamedParameter("CP", codProd);
        fifo.setNamedParameter("CV", codVol);
        fifo.setNamedParameter("CL", codLocal);

        java.sql.Date dtOld;
        try (ResultSet rs = fifo.executeQuery()) {
            if (!rs.next() || rs.getDate("DTPROD_OLD") == null) return;
            dtOld = rs.getDate("DTPROD_OLD");
        }

        if (!dtProd.after(dtOld)) return;

    /* ===========================================================
       5️⃣ FIFO INVÁLIDO → CONTABILIZA TENTATIVA
       =========================================================== */
        tentativas++;

        String status = tentativas >= 3 ? "P" : "N";

// 🔥 LOG EM TRANSAÇÃO AUTÔNOMA (NÃO SOFRE ROLLBACK)
        registrarLogFIFO(nuNotaOrig, codProd, tentativas, status);

        throw new MGEModelException(
                tentativas >= 3
                        ? "FIFO inválido. Limite de tentativas atingido. Aguardando liberação."
                        : "FIFO inválido. Existe etiqueta mais antiga: "
                        + new java.text.SimpleDateFormat("dd/MM/yyyy").format(dtOld) + "."
        );

    }


    private void registrarLogFIFO(BigDecimal nuConf,
                                  BigDecimal codProd,
                                  int tentativas,
                                  String liberacao) throws Exception {

        JapeSession.SessionHandle hnd = null;
        JdbcWrapper jdbc = null;

        try {
            // 🔥 TRANSAÇÃO AUTÔNOMA COM AUTOCOMMIT
            hnd = JapeSession.open(true);
            jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
            jdbc.openSession();

            NativeSql upd = new NativeSql(jdbc);
            upd.appendSql(
                    "MERGE INTO AD_FTILOGLIBFIFOBIP L " +
                            "USING DUAL ON (L.NUNOTA = :N AND L.CODPROD = :CP) " +
                            "WHEN MATCHED THEN " +
                            "  UPDATE SET TENTATIVAS = :T, LIBERACAO = :L " +
                            "WHEN NOT MATCHED THEN " +
                            "  INSERT (NROCONFIG, NUNOTA, CODPROD, TENTATIVAS, LIBERACAO) " +
                            "  VALUES (1, :N, :CP, :T, :L)"
            );

            upd.setNamedParameter("N", nuConf);
            upd.setNamedParameter("CP", codProd);
            upd.setNamedParameter("T", tentativas);
            upd.setNamedParameter("L", liberacao);

            upd.executeUpdate(); // 👈 já COMMITA sozinho

        } finally {
            JdbcWrapper.closeSession(jdbc);
            JapeSession.close(hnd);
        }
    }


}
