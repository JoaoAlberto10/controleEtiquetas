import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class TRG_CONF_ITEM implements EventoProgramavelJava {

    @Override public void afterDelete(PersistenceEvent event) throws Exception {}
    @Override public void beforeCommit(TransactionContext tranCtx) throws Exception {}
    @Override public void beforeDelete(PersistenceEvent event) throws Exception {}
    @Override public void afterInsert(PersistenceEvent event) throws Exception {}
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void beforeUpdate(PersistenceEvent event) throws Exception {}

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        incluirItemNota(event);
    }

    private void incluirItemNota(PersistenceEvent event) throws Exception {
        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();
        EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();

        BigDecimal nuConf = vo.asBigDecimal("NUCONF");
        if (nuConf == null)
            throw new Exception("NUCONF não informado no registro auxiliar!");

        Map<String, Object> conferencia = obterConferencia(jdbc, nuConf);
        BigDecimal nuNotaConf = conferencia == null ? null : (BigDecimal) conferencia.get("NUNOTACONF");

        if (nuNotaConf == null) {
            BigDecimal nuNota = gerarNotaConferencia(dwfFacade, jdbc, nuConf, conferencia);
            incluirItemConferencia(dwfFacade, vo, nuNota);
        } else {
            incluirItemConferencia(dwfFacade, vo, nuNotaConf);
        }
    }

    private Map<String, Object> obterConferencia(JdbcWrapper jdbc, BigDecimal nuConf) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT * FROM AD_FTICONFERENCIA WHERE NUCONF = :NUCONF");
        ns.setNamedParameter("NUCONF", nuConf);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                map.put("NUNOTACONF", rs.getBigDecimal("NUNOTACONF"));
                map.put("STATUS", rs.getString("STATUS"));
                map.put("NUNOTAORIG", rs.getBigDecimal("NUNOTAORIG"));
                map.put("FORMAVOLUMES", rs.getString("FORMAVOLUMES"));
                map.put("CODLOCAL", rs.getBigDecimal("CODLOCAL"));
                map.put("CODLOCALDEST", rs.getBigDecimal("CODLOCALDEST"));
                map.put("TIPMOV", rs.getString("TIPMOV"));
                map.put("TOPCONF", rs.getBigDecimal("TOPCONF"));
                return map;
            }
        }
        return null;
    }

    private BigDecimal gerarNotaConferencia(EntityFacade dwfFacade, JdbcWrapper jdbc, BigDecimal nuConf, Map conferencia) throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        DynamicVO cabVO = (DynamicVO) dwfFacade.getDefaultValueObjectInstance(DynamicEntityNames.CABECALHO_NOTA);
        cabVO.setProperty("CODEMP", BigDecimal.ONE);
        cabVO.setProperty("DTNEG", new Timestamp(cal.getTimeInMillis()));
        cabVO.setProperty("CODPARC", new BigDecimal(999996));
        cabVO.setProperty("NUMNOTA", BigDecimal.ZERO);
        cabVO.setProperty("CODTIPOPER", conferencia.get("TOPCONF"));
        cabVO.setProperty("AD_CODLOCALORIG", conferencia.get("CODLOCAL"));
        cabVO.setProperty("AD_CODLOCALDEST",conferencia.get("CODLOCALDEST"));

        dwfFacade.createEntity(DynamicEntityNames.CABECALHO_NOTA, (EntityVO) cabVO);

        BigDecimal nuNota = cabVO.asBigDecimal("NUNOTA");
        atualizarNotaConf(jdbc, nuNota, nuConf);

        return nuNota;
    }

    private void atualizarNotaConf(JdbcWrapper jdbc, BigDecimal nuNota, BigDecimal nuConf) throws Exception {
        NativeSql upConf = new NativeSql(jdbc);
        upConf.appendSql("UPDATE AD_FTICONFERENCIA SET NUNOTACONF = :NUNOTA WHERE NUCONF = :NUCONF");
        upConf.setNamedParameter("NUNOTA", nuNota);
        upConf.setNamedParameter("NUCONF", nuConf);
        upConf.executeUpdate();
    }

    private void incluirItemConferencia(EntityFacade dwfFacade, DynamicVO vo, BigDecimal nuNota) throws Exception {
        DynamicVO iteVO = (DynamicVO) dwfFacade.getDefaultValueObjectInstance(DynamicEntityNames.ITEM_NOTA);
        iteVO.setProperty("NUNOTA", nuNota);
        iteVO.setProperty("CODPROD", vo.asBigDecimal("CODPROD"));
        iteVO.setProperty("CODVOL", vo.asString("CODVOL"));
        iteVO.setProperty("CODBARRAPDV", vo.asString("CODBARRAPDV"));
        iteVO.setProperty("QTDNEG", vo.asBigDecimal("QTDNEG"));
        iteVO.setProperty("CODLOCALORIG", vo.asBigDecimal("CODLOCALORIG"));
        dwfFacade.createEntity(DynamicEntityNames.ITEM_NOTA, (EntityVO) iteVO);
    }
}
