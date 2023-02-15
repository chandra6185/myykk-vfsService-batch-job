package com.myykk.dao;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import com.myykk.model.BuildDemandDtl;
import com.myykk.model.BuildDemandHdr;
import com.myykk.model.BuildDemandTxt;
import com.myykk.utility.Util;
import com.myykk.utility.YKKConstants;

@Repository
public class VfsRepository {

	private static final Logger log = LoggerFactory.getLogger(VfsRepository.class);

	@Value("${jdbc.batch.email.schemaname}")
	String schemaname;

	@Autowired
	private final NamedParameterJdbcTemplate jdbcTemplate;
	public VfsRepository(NamedParameterJdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void saveVFSForecastData(List<BuildDemandHdr> dmdHdr, List<BuildDemandDtl> dmdDtl, List<BuildDemandTxt> dmdTxt) throws SQLException {

		Map<String,Object>[] hdrBatchOfInputs = new HashMap[dmdHdr.size()];
		Map<String,Object>[] dtlBatchOfInputs = new HashMap[dmdDtl.size()];
		Map<String,Object>[] txtBatchOfInputs = new HashMap[dmdTxt.size()];

		int count = 0;
		for(BuildDemandHdr hdr : dmdHdr) {
			Map<String,Object> hdrMap = new HashMap<String, Object>();
			hdrMap.put("VVND3H", hdr.getVendorH());
			hdrMap.put("VITM3H", hdr.getMatnrH());
			hdrMap.put("VREC3H",hdr.getRecType1H());
			hdrMap.put("VDES3H",hdr.getMatDescrH());
			hdrMap.put("VYKK3H",hdr.getYKKMatnrH());
			hdrMap.put("VYKN3H",hdr.getYKKNameH());
			hdrMap.put("VDTS3H",hdr.getBeginDateH());
			hdrMap.put("VDTE3H",hdr.getEndDateH());
			hdrMap.put("VQTY3H",hdr.getSafetyStockH());
			hdrMap.put("VCHK3H",hdr.getCheckTotalH());
			hdrMap.put("VD013H",hdr.getbDate1H());
			hdrMap.put("VQ013H",hdr.getPbckt1H());
			hdrMap.put("VD023H",hdr.getbDate2H());
			hdrMap.put("VQ023H", hdr.getPbckt2H());
			hdrMap.put("VD033H", hdr.getbDate3H());
			hdrMap.put("VQ033H", hdr.getPbckt3H());
			hdrMap.put("VD043H", hdr.getbDate4H());
			hdrMap.put("VQ043H", hdr.getPbckt4H());
			hdrMap.put("VD053H", hdr.getbDate5H());
			hdrMap.put("VQ053H", hdr.getPbckt5H());
			hdrMap.put("VD063H", hdr.getbDate6H());
			hdrMap.put("VQ063H", hdr.getPbckt6H());
			hdrMap.put("VD073H", hdr.getbDate7H());
			hdrMap.put("VQ073H", hdr.getPbckt7H());
			hdrMap.put("VD083H", hdr.getbDate8H());
			hdrMap.put("VQ083H", hdr.getPbckt8H());
			hdrMap.put("VD093H", hdr.getbDate9H());
			hdrMap.put("VQ093H", hdr.getPbckt9H());
			hdrMap.put("VD103H", hdr.getbDate10H());
			hdrMap.put("VQ103H", hdr.getPbckt10H());
			hdrMap.put("VD113H", hdr.getbDate11H());
			hdrMap.put("VQ113H", hdr.getPbckt11H());
			hdrMap.put("VD123H", hdr.getbDate12H());
			hdrMap.put("VQ123H", hdr.getPbckt12H());
			hdrMap.put("VD133H", hdr.getbDate13H());
			hdrMap.put("VQT13H", hdr.getPbckt13H());
			hdrMap.put("VD143H", hdr.getbDate14H());
			hdrMap.put("VQ143H", hdr.getPbckt14H());
			hdrMap.put("VD153H", hdr.getbDate15H());
			hdrMap.put("VQ153H", hdr.getPbckt15H());
			hdrMap.put("VUOM3H", hdr.getBaseUOMH());
			hdrMap.put("VORU3H", hdr.getOrderUOMH());
			hdrMap.put("VCUS3H", hdr.getCustNumH());
			hdrMap.put("VFRD3H", hdr.getDocRcvDateH());
			hdrMap.put("VFRT3H", hdr.getDocRcvTimeH());
			hdrMap.put("VSER3H", hdr.getSerializedRecNumH());
			hdrMap.put("VPAR3H", hdr.getPartnerIDH());
			hdrMap.put("VFL13H", hdr.getProcessFlag1H());
			hdrMap.put("VFL23H", hdr.getProcessFlag2H());
			hdrMap.put("VEXT3H", hdr.getHolderField1H());
			hdrMap.put("VRL#3H", hdr.getThisRelNumH());
			hdrMap.put("VTY13H", hdr.getThisRelCntT1H());
			hdrMap.put("VTY23H", hdr.getThisRelCntT2H());
			hdrMap.put("VTY33H", hdr.getThisRelCntT3H());
			hdrMap.put("VID3H", hdr.getTimeStampH());
			
			hdrBatchOfInputs[count] = hdrMap;
			
			++count;
		}
		
		count = 0;
		for(BuildDemandDtl dtl : dmdDtl) {
			
			Map<String,Object> dtlMap = new HashMap<String, Object>();
			
			dtlMap.put("VVND3D", dtl.getVendorD());
			dtlMap.put("VITM3D", dtl.getMatnrD());
			dtlMap.put("VREC3D", dtl.getRecType1D());
			dtlMap.put("VPLA3D", dtl.getPlantIDD());
			dtlMap.put("VPLN3D", dtl.getPlantNameD());
			dtlMap.put("VDTS3D", dtl.getBeginDateD());
			dtlMap.put("VDTE3D", dtl.getEndDateD());
			dtlMap.put("VQTY3D", dtl.getSafetyStockD());
			dtlMap.put("VCHK3D", dtl.getCheckTotalD());
			dtlMap.put("VD013D", dtl.getbDate1D());
			dtlMap.put("VQ013D", dtl.getPbckt1D());
			dtlMap.put("VD023D", dtl.getbDate2D());
			dtlMap.put("VQ023D", dtl.getPbckt2D());
			dtlMap.put("VD033D", dtl.getbDate3D());
			dtlMap.put("VQ033D", dtl.getPbckt3D());
			dtlMap.put("VD043D", dtl.getbDate4D());
			dtlMap.put("VQ043D", dtl.getPbckt4D());
			dtlMap.put("VD053D", dtl.getbDate5D());
			dtlMap.put("VQ053D", dtl.getPbckt5D());
			dtlMap.put("VD063D", dtl.getbDate6D());
			dtlMap.put("VQ063D", dtl.getPbckt6D());
			dtlMap.put("VD073D", dtl.getbDate7D());
			dtlMap.put("VQ073D", dtl.getPbckt7D());
			dtlMap.put("VD083D", dtl.getbDate8D());
			dtlMap.put("VQ083D", dtl.getPbckt8D());
			dtlMap.put("VD093D", dtl.getbDate9D());
			dtlMap.put("VQ093D", dtl.getPbckt9D());
			dtlMap.put("VD103D", dtl.getbDate10D());
			dtlMap.put("VQ103D", dtl.getPbckt10D());
			dtlMap.put("VD113D", dtl.getbDate11D());
			dtlMap.put("VQ113D", dtl.getPbckt11D());
			dtlMap.put("VD123D", dtl.getbDate12D());
			dtlMap.put("VQ123D", dtl.getPbckt12D());
			dtlMap.put("VD133D", dtl.getbDate13D());
			dtlMap.put("VQ133D", dtl.getPbckt13D());
			dtlMap.put("VD143D", dtl.getbDate14D());
			dtlMap.put("VQ143D", dtl.getPbckt14D());
			dtlMap.put("VD153D", dtl.getbDate15D());
			dtlMap.put("VQ153D", dtl.getPbckt15D());
			dtlMap.put("VUOM3D", dtl.getBaseUOMD());
			dtlMap.put("VORO3D", dtl.getOrderUOMD());
			dtlMap.put("VCUS3D", dtl.getCustNumD());
			dtlMap.put("VFRD3D", dtl.getDocRcvDateD());
			dtlMap.put("VFRT3D", dtl.getDocRcvTimeD());
			dtlMap.put("VSER3D", dtl.getSerializedRecNumD());
			dtlMap.put("VPAR3D", dtl.getPartnerIDD());
			dtlMap.put("VFL13D", dtl.getProcessFlag1D());
			dtlMap.put("VFL23D", dtl.getProcessFlag2D());
			dtlMap.put("VEXT3D", dtl.getHolderField1D());
			dtlMap.put("VRL#3D", dtl.getThisRelNumD());
			dtlMap.put("VTY13D", dtl.getThisRelCntT1D());
			dtlMap.put("VTY23D", dtl.getThisRelCntT2D());
			dtlMap.put("VTY33D", dtl.getThisRelCntT3D());
			dtlMap.put("VID3D", dtl.getTimeStampD());
			
						
			dtlBatchOfInputs[count] = dtlMap;
			++count;
		}
		
		count = 0;

		for(BuildDemandTxt txt : dmdTxt) {
			
			Map<String,Object> txtMap = new HashMap<String, Object>();
			
			txtMap.put("VVND3T", txt.getVendorX());
			txtMap.put("VITM3T", txt.getMatnrX());
			txtMap.put("VREC3T", txt.getRecType1X());
			txtMap.put("VMS13T", txt.getTxt1X());
			txtMap.put("VMS23T", txt.getTxt2X());
			txtMap.put("VMS33T", txt.getTxt3X());
			txtMap.put("VMS43T", txt.getTxt4X());
			txtMap.put("VMS53T", txt.getTxt5X());
			txtMap.put("VMS63T", txt.getTxt6X());
			txtMap.put("VMS73T", txt.getTxt7X());
			txtMap.put("VCUS3T", txt.getCustNumX());
			txtMap.put("VFRD3T", txt.getDocRcvDateX());
			txtMap.put("VFRT3T", txt.getDocRcvTimeX());
			txtMap.put("VSER3T", txt.getSerializedRecNumX());
			txtMap.put("VPAR3T", txt.getPartnerIDX());
			txtMap.put("VFL13T", txt.getProcessFlag1X());
			txtMap.put("VFL23T", txt.getProcessFlag2X());
			txtMap.put("VEXT3T", txt.getHolderField1X());
			txtMap.put("VRL#3T", txt.getThisRelNumX());
			txtMap.put("VTY13T", txt.getThisRelCntT1X());
			txtMap.put("VTY23T", txt.getThisRelCntT2X());
			txtMap.put("VTY33T", txt.getThisRelCntT3X());
			txtMap.put("VID3T", txt.getTimeStampX());
			
			txtBatchOfInputs[count] = txtMap;
			++count;
		}

		if(hdrBatchOfInputs.length > 0 ) {
			int noOfRowsHdr[] = saveVFSBatchData(getVFSHeaderQuery(), hdrBatchOfInputs);
			log.debug("No of Header Rows inserted :" + noOfRowsHdr.length);
		}
		if(dtlBatchOfInputs.length > 0) {
			int noOfRowsDtl[] = saveVFSBatchData(getVFSDetailQuery(), dtlBatchOfInputs);
			log.debug("No of Details Rows inserted :" + noOfRowsDtl.length);
		}
		if(txtBatchOfInputs.length > 0) {
			int noOfRowsTxt[] = saveVFSBatchData(getVFSTxtQuery(), txtBatchOfInputs);
			log.debug("No of Txt Rows inserted : "  + noOfRowsTxt.length);
		}
	}

	public String getVFSHeaderQuery() {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ")
				.append(schemaname)
				.append(".VMI3HDR (VVND3H,VITM3H,VREC3H,VDES3H,VYKK3H,VYKN3H,VDTS3H,VDTE3H,")
				.append("VQTY3H,VCHK3H,VD013H,VQ013H,VD023H,VQ023H,VD033H,VQ033H,VD043H,")
				.append("VQ043H,VD053H,VQ053H,VD063H,VQ063H,VD073H,VQ073H,VD083H,VQ083H,")
				.append("VD093H,VQ093H,VD103H,VQ103H,VD113H,VQ113H,VD123H,VQ123H,VD133H," )
				.append("VQT13H,VD143H,VQ143H,VD153H,VQ153H,VUOM3H,VORU3H,VCUS3H,VFRD3H,")
				.append("VFRT3H,VSER3H,VPAR3H,VFL13H,VFL23H,VEXT3H,VRL#3H,VTY13H,VTY23H,VTY33H,VID3H)")
				.append(" values(:VVND3H, :VITM3H, :VREC3H, :VDES3H, :VYKK3H, :VYKN3H, :VDTS3H, :VDTE3H, ")
				.append(":VQTY3H, :VCHK3H, :VD013H, :VQ013H, :VD023H, :VQ023H, :VD033H, :VQ033H, :VD043H, ")						
				.append(":VQ043H, :VD053H, :VQ053H, :VD063H, :VQ063H, :VD073H, :VQ073H, :VD083H, :VQ083H, ")
				.append(":VD093H, :VQ093H, :VD103H, :VQ103H, :VD113H, :VQ113H, :VD123H, :VQ123H, :VD133H, ")
				.append(":VQT13H, :VD143H, :VQ143H, :VD153H, :VQ153H, :VUOM3H, :VORU3H, :VCUS3H, :VFRD3H, ")					
				.append(":VFRT3H, :VSER3H, :VPAR3H, :VFL13H, :VFL23H, :VEXT3H, :VRL#3H, :VTY13H, :VTY23H, :VTY33H, :VID3H)");
		return sb.toString();
		
		
	}

	public String getVFSDetailQuery() {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ")
				.append(schemaname)
				.append(".VMI3DTL (VVND3D,VITM3D,VREC3D,VPLA3D,VPLN3D,VDTS3D,VDTE3D,VQTY3D,VCHK3D,")
				.append("VD013D,VQ013D,VD023D,VQ023D,VD033D,VQ033D,VD043D,VQ043D,VD053D,VQ053D,VD063D,VQ063D,VD073D,")
				.append("VQ073D,VD083D,VQ083D,VD093D,VQ093D,VD103D,VQ103D,VD113D,VQ113D,VD123D,VQ123D,VD133D,VQ133D,VD143D,")
				.append("VQ143D,VD153D,VQ153D,VUOM3D,VORO3D,VCUS3D,VFRD3D,VFRT3D,VSER3D,VPAR3D,VFL13D,VFL23D,VEXT3D,VRL#3D,VTY13D,VTY23D,VTY33D,VID3D)")
				.append("values(:VVND3D, :VITM3D, :VREC3D, :VPLA3D, :VPLN3D, :VDTS3D, :VDTE3D, :VQTY3D, :VCHK3D, ")
				.append(":VD013D, :VQ013D, :VD023D, :VQ023D, :VD033D, :VQ033D, :VD043D, :VQ043D, :VD053D, :VQ053D, :VD063D, :VQ063D, :VD073D, ")
				.append(":VQ073D, :VD083D, :VQ083D, :VD093D, :VQ093D, :VD103D, :VQ103D, :VD113D, :VQ113D, :VD123D, :VQ123D, :VD133D, :VQ133D, :VD143D, ")
				.append(":VQ143D, :VD153D, :VQ153D, :VUOM3D, :VORO3D, :VCUS3D, :VFRD3D, :VFRT3D, :VSER3D, :VPAR3D, :VFL13D, :VFL23D, :VEXT3D, :VRL#3D, ")
				.append(":VTY13D, :VTY23D, :VTY33D, :VID3D)");
				

		return sb.toString();
	}

	public String getVFSTxtQuery() {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ")
				.append(schemaname)
				.append(".VMI3TXT (VVND3T,VITM3T,VREC3T,VMS13T,VMS23T,VMS33T,VMS43T,VMS53T,VMS63T,VMS73T,VCUS3T," )
				.append("VFRD3T,VFRT3T,VSER3T,VPAR3T,VFL13T,VFL23T,VEXT3T,VRL#3T,VTY13T,VTY23T,VTY33T,VID3T)" )
				.append("values(:VVND3T, :VITM3T, :VREC3T, :VMS13T, :VMS23T, :VMS33T, :VMS43T, :VMS53T, :VMS63T, :VMS73T, :VCUS3T, ")
				.append(":VFRD3T, :VFRT3T, :VSER3T, :VPAR3T, :VFL13T, :VFL23T, :VEXT3T, :VRL#3T, :VTY13T, :VTY23T, :VTY33T, :VID3T)");
		return sb.toString();
	}

	public String getVFSStatQuery() {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ")
				.append(schemaname)
				.append(".YOUTSTAT (HSTPID,HSREGN,HSDEPO,HSCUST,HSSHP#,HSPONO,HSASNO,HSRLNO,HSPDAT,")
				.append("HSYKO#,HSRFQL,HSRFNO,HSEVNT,HSSTA1,HSPRIR,HSDTE1,HSTIM1,")
				.append("HSCNST,HSDTE2,HSTIM2,HSDTQ3,HSDTE3,HSTIM3,HSDTQ4,HSDTE4,HSTIM4,")
				.append("HSDTQ5,HSDTE5,HSTIM5,HSDTQ6,HSDTE6,HSTIM6,HSDTQ7,HSDTE7,HSTIM7,")
				.append("HSDTQ8,HSDTE8,HSTIM8,HSIDQ1,HSIDC1,HSLINS,HS@PO,HS@MBX,HS@IRF,")
				.append("HS@GRF,HS@TRF,HS@ISA,HSRECT) values (:HSTPID, :HSREGN, :HSDEPO, :HSCUST, :HSSHP#, :HSPONO, :HSASNO, :HSRLNO, :HSPDAT, ")
				.append(":HSYKO#, :HSRFQL, :HSRFNO, :HSEVNT, :HSSTA1, :HSPRIR, :HSDTE1, :HSTIM1, ")
				.append(":HSCNST, :HSDTE2, :HSTIM2, :HSDTQ3, :HSDTE3, :HSTIM3, :HSDTQ4, :HSDTE4, :HSTIM4,")
				.append(":HSDTQ5, :HSDTE5, :HSTIM5, :HSDTQ6, :HSDTE6, :HSTIM6, :HSDTQ7, :HSDTE7, :HSTIM7, ")
				.append(":HSDTQ8, :HSDTE8, :HSTIM8, :HSIDQ1, :HSIDC1, :HSLINS, :HS@PO, :HS@MBX, :HS@IRF, ")
				.append(":HS@GRF, :HS@TRF, :HS@ISA, :HSRECT)");
		return sb.toString();

	}


	public int[] saveVFSBatchData(final String query, final Map<String, Object>[] batchValues){

		return jdbcTemplate.batchUpdate(query, batchValues);
	}

	public void saveVFSStatus(String releaseNumber) throws SQLException {

		Calendar cal = Calendar.getInstance();
		String formattedDate = Util.formatDateTo(cal.getTime(), "yyyyMMdd");
		String formattedTime = Util.formatDateTo(cal.getTime(), "yyyyMMddHHmmss");
		
		Map<String,Object>[] batchValues = new HashMap[1];
		
		batchValues[0] = new HashMap<String, Object>();
		
		batchValues[0].put("HSTPID", YKKConstants.THIS_PARTNER);		
		batchValues[0].put("HSREGN", "");
		batchValues[0].put("HSDEPO", "");
		batchValues[0].put("HSCUST", YKKConstants.VFS_CUST_NO);
		batchValues[0].put("HSSHP#", "---");
		batchValues[0].put("HSPONO", "");
		batchValues[0].put("HSASNO", "");
		batchValues[0].put("HSRLNO", releaseNumber);
		batchValues[0].put("HSPDAT", 0);
		batchValues[0].put("HSYKO#", "");
		batchValues[0].put("HSRFQL", "F");
		batchValues[0].put("HSRFNO", "");
		batchValues[0].put("HSEVNT", "1");
		batchValues[0].put("HSSTA1", "");
		batchValues[0].put("HSPRIR", "");
		batchValues[0].put("HSDTE1", Integer.parseInt(formattedDate));
		batchValues[0].put("HSTIM1", Util.getTimeStamp(cal));
		batchValues[0].put("HSCNST", "");
		batchValues[0].put("HSDTE2", 0);
		batchValues[0].put("HSTIM2", 0);
		batchValues[0].put("HSDTQ3", "");
		batchValues[0].put("HSDTE3", 0);
		batchValues[0].put("HSTIM3", 0);
		batchValues[0].put("HSDTQ4", "");
		batchValues[0].put("HSDTE4", 0);
		batchValues[0].put("HSTIM4", 0);
		batchValues[0].put("HSDTQ5", "");
		batchValues[0].put("HSDTE5", 0);
		batchValues[0].put("HSTIM5", 0);
		batchValues[0].put("HSDTQ6", "");
		batchValues[0].put("HSDTE6", 0);
		batchValues[0].put("HSTIM6", 0);
		batchValues[0].put("HSDTQ7", "");
		batchValues[0].put("HSDTE7", 0);
		batchValues[0].put("HSTIM7", 0);
		batchValues[0].put("HSDTQ8", "");
		batchValues[0].put("HSDTE8", 0);
		batchValues[0].put("HSTIM8", 0);
		batchValues[0].put("HSIDQ1", "");
		batchValues[0].put("HSIDC1", "");
		batchValues[0].put("HSLINS", 0);
		batchValues[0].put("HS@PO", "");
		batchValues[0].put("HS@MBX", "");
		batchValues[0].put("HS@IRF", "");
		batchValues[0].put("HS@GRF", "");
		batchValues[0].put("HS@TRF", "");
		batchValues[0].put("HS@ISA", formattedTime);
		batchValues[0].put("HSRECT", "EDI3");	
		
		jdbcTemplate.batchUpdate(getVFSStatQuery(), batchValues);

	}
}
