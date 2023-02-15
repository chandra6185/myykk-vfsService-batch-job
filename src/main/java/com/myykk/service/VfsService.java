package com.myykk.service;

import com.myykk.dao.VfsRepository;
import com.myykk.model.BuildDemandDtl;
import com.myykk.model.BuildDemandHdr;
import com.myykk.model.BuildDemandTxt;
import com.myykk.utility.Util;
import com.myykk.utility.YKKConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.springframework.transaction.annotation.Transactional;

@Service
public class VfsService {

	private static final Logger log = LoggerFactory.getLogger(VfsService.class);
	
	@Autowired
	VfsRepository vfsRepository;

	@Value("${vfs.input.dir}")
	String inputDirectory;

	@Value("${vfs.inprocess.dir}")
	String inprocessDirectory;

	@Value("${vfs.success.dir}")
	String successDirectory;

	@Value("${vfs.error.dir}")
	String errorDirectory;

	public void readAndSaveVFS() {

		log.debug("Entering readVFSDirectory method to read ");
		File dir = new File(inputDirectory);
		File destDir = new File(inprocessDirectory);
		File successDir = new File(successDirectory);
		File errorDir = new File(errorDirectory);
		String extensions[] = new String[]{"dat"};
		//String extensions[] = new String[]{"txt"};
		File toRead = null;
		List<File> fileList = new ArrayList<File>(FileUtils.listFiles(dir, extensions, false));
		try {
			for(File file : fileList) {
				log.debug("File to String " + file.toString());
				FileUtils.copyFileToDirectory(file, destDir,true);
				toRead = new File(destDir+"/"+file.getName());
				FileUtils.forceDelete(file);
				String relNumber = saveVFSData(toRead.toString(), Util.formatDateTo(Calendar.getInstance().getTime(), "yyyyMMddHHmmss"));
				if(relNumber != null) {
					saveVFSStatus(relNumber);
					FileUtils.copyFileToDirectory(toRead, successDir);
				} else {
					FileUtils.copyFileToDirectory(toRead, errorDir);
				}
				FileUtils.forceDelete(toRead);

			}
		}catch(Exception e ) {
			e.printStackTrace();
			log.error("Caught Exception " + e.getMessage());
			try {
				FileUtils.copyFileToDirectory(toRead, errorDir);
				FileUtils.forceDelete(toRead);
			}catch(Exception  se) {
				log.error("Error while looping through input directory");

			}
		}
	}

	@Transactional
	public  String saveVFSData(String fileNameIn, String tdStamp) {
		log.debug("fileNameIn :" + fileNameIn);
		String relNumber = "";
		BufferedReader in = null;
		
		try {
			in = new BufferedReader(new FileReader(fileNameIn));
			String line;
			int yCount = 0;
			int rCount = 0;
			StringTokenizer st = null;
			Calendar cal = Calendar.getInstance();
			String wbDate = Util.getYMD(4);
			String findThis = "||";
			String replaceWith = "| |";
			String rpString = null;
			String recType = null;
			String thisReleaseNumber = null;
			String thisReleaseType1RecsCount = null;
			String thisReleaseType2RecsCount = null;
			String thisReleaseType3RecsCount = null;
			String data = null;
			String vendor = null;
			String matnr = null;
			int fieldLoopCount = 0;

			BuildDemandHdr bldDmdH = null;
			BuildDemandDtl bldDmdD = null;
			BuildDemandTxt bldDmdX = null;
			List<BuildDemandHdr> dmdHdr1 = new ArrayList<BuildDemandHdr>();
			List<BuildDemandDtl> dmdDtl1  = new ArrayList<BuildDemandDtl>();
			List<BuildDemandTxt> dmdTxt1= new ArrayList<BuildDemandTxt>();

			String useThisUOM = null;
			while ((line = in.readLine()) != null) {
				rpString = Util.replaceAll(line, findThis, replaceWith);
				rpString = Util.replaceAll(rpString, findThis, replaceWith);
				//LOG.debug("Line after replacement : " + rpString);
				st = new StringTokenizer(rpString.toUpperCase(), "|",
						false);
				fieldLoopCount = 0;
				rCount = 0;
				if (yCount == 0) {
					while (st.hasMoreTokens()) {
						recType = st.nextToken();
						if (fieldLoopCount == 2) {
							if(recType.equalsIgnoreCase("9")) {
								thisReleaseType1RecsCount = st.nextToken();
								thisReleaseType2RecsCount = st.nextToken();
								thisReleaseType3RecsCount = st.nextToken();
								thisReleaseNumber = st.nextToken();
								relNumber = thisReleaseNumber;
							}
						}
						fieldLoopCount++;
					}
				}else
					fieldLoopCount = 0;
				{
					while (st.hasMoreTokens()) {
						data = st.nextToken();
						if (fieldLoopCount == 0) {
							vendor = data;
						}
						if (fieldLoopCount == 1) {
							matnr = data;
						}
						if (fieldLoopCount == 6 || fieldLoopCount == 7 || fieldLoopCount == 8) {

						}

						if (fieldLoopCount == 2) {
							recType = data;
							if (recType.equalsIgnoreCase("1")) {
								bldDmdH = new BuildDemandHdr();
								//String getSafetyStock = null;

								bldDmdH.setThisRelCntT1H(thisReleaseType1RecsCount);
								bldDmdH.setThisRelCntT2H(thisReleaseType2RecsCount);
								bldDmdH.setThisRelCntT3H(thisReleaseType3RecsCount);
								bldDmdH.setThisRelNumH(thisReleaseNumber);
								bldDmdH.setVendorH(vendor);
								bldDmdH.setMatnrH(matnr);
								bldDmdH.setRecType1H(recType);
								rCount++;
								bldDmdH.setMatDescrH(st.nextToken());
								rCount++;
								bldDmdH.setYKKMatnrH(st.nextToken());
								rCount++;
								bldDmdH.setYKKNameH(st.nextToken());
								rCount++;
								bldDmdH.setBeginDateH(st.nextToken());
								rCount++;
								bldDmdH.setEndDateH(st.nextToken());
								String ttest = st.nextElement().toString();
								String blankStock = " ";
								if (ttest.equalsIgnoreCase(blankStock)) {
									bldDmdH.setSafetyStockH("0");
								} else {
									bldDmdH.setSafetyStockH(ttest);
								}
								rCount++;
								bldDmdH.setCheckTotalH(st.nextToken());

								rCount++;
								bldDmdH.setbDate1H(st.nextToken());
								fieldLoopCount++;
								rCount++;
								bldDmdH.setPbckt1H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate2H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt2H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate3H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt3H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate4H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt4H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate5H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt5H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate6H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt6H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate7H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt7H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate8H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt8H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate9H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt9H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate10H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt10H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate11H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt11H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate12H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt12H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate13H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt13H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate14H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt14H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setbDate15H(st.nextToken());
								fieldLoopCount++;
								bldDmdH.setPbckt15H(st.nextToken());
								fieldLoopCount++;
								String testForBaseUOM=st.nextToken();
								fieldLoopCount++;
								String testForOrderUOM=st.nextToken();
								if(!testForOrderUOM.equalsIgnoreCase(" ")) {
									bldDmdH.setBaseUOMH(testForOrderUOM);
									bldDmdH.setOrderUOMH(testForOrderUOM);
									useThisUOM=testForOrderUOM;
								}else {
									bldDmdH.setBaseUOMH(testForBaseUOM);
									bldDmdH.setOrderUOMH(testForBaseUOM);
									useThisUOM = testForBaseUOM;
								}

								fieldLoopCount++;
								bldDmdH.setCustNumH("000000");
								bldDmdH.setDocRcvDateH(wbDate);
								bldDmdH.setDocRcvTimeH(String.valueOf(Util.getTimeStamp(cal)));
								bldDmdH.setSerializedRecNumH("0000000");
								bldDmdH.setPartnerIDH(YKKConstants.THIS_PARTNER);
								bldDmdH.setProcessFlag1H(" ");
								bldDmdH.setProcessFlag2H(" ");
								bldDmdH.setHolderField1H(" ");
								bldDmdH.setTimeStampH(tdStamp);

								dmdHdr1.add(bldDmdH);

								// continue;
							} // end recType=1 (create the records from the record type)
							else if (recType.equalsIgnoreCase("2")) {
								bldDmdX = new BuildDemandTxt();
								bldDmdX.setVendorX(vendor);
								bldDmdX.setMatnrX(matnr);
								bldDmdX.setRecType1X(recType);
								bldDmdX.setCustNumX("000000");
								bldDmdX.setDocRcvDateX(wbDate);
								bldDmdX.setDocRcvTimeX(String.valueOf(Util.getTimeStamp(cal)));
								bldDmdX.setSerializedRecNumX("0000000");
								bldDmdX.setPartnerIDX(YKKConstants.THIS_PARTNER);
								bldDmdX.setProcessFlag1X(" ");
								bldDmdX.setProcessFlag2X(" ");
								bldDmdX.setHolderField1X(" ");
								bldDmdX.setThisRelCntT1X(thisReleaseType1RecsCount);
								bldDmdX.setThisRelCntT2X(thisReleaseType2RecsCount);
								bldDmdX.setThisRelCntT3X(thisReleaseType3RecsCount);
								bldDmdX.setThisRelNumX(thisReleaseNumber);
								bldDmdX.setTimeStampX(tdStamp);
								String nextText = null;
								int y = 0;
								// for (int y = 0; y < 9; y++) {
								while (st.hasMoreElements()) {
									nextText = st.nextToken();
									/*
									 * VF documentation says there are 7 text
									 * fields, but the actual file has 9.
									 */
									//int nxTxt = nextText.length();
									//System.out.println("T = " + nxTxt
									//		+ (nextText.trim()));
									if (y == 0) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt1X(nextText);
										} else
											bldDmdX.setTxt1X(" ");
									} else if (y == 1) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt2X(nextText);
										} else
											bldDmdX.setTxt2X(" ");
									} else if (y == 2) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt3X(nextText);
										} else
											bldDmdX.setTxt3X(" ");
									} else if (y == 3) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt4X(nextText);
										} else
											bldDmdX.setTxt4X(" ");
									} else if (y == 4) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt5X(nextText);
										} else
											bldDmdX.setTxt5X(" ");
									} else if (y == 5) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt6X(nextText);
										} else
											bldDmdX.setTxt6X(" ");
									} else if (y == 6) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt7X(nextText);
										} else
											bldDmdX.setTxt7X(" ");
									} else if (y == 7) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt8X(nextText);
										} else
											bldDmdX.setTxt8X(" ");
									} else if (y == 8) {
										if (!(nextText.equalsIgnoreCase(null))) {
											bldDmdX.setTxt9X(nextText);
										} else
											bldDmdX.setTxt9X(" ");
									}
									y++;
								}
								dmdTxt1.add(bldDmdX);
							}
							// end recType = '2'
							else if (recType.equalsIgnoreCase("3")) {
								bldDmdD = new BuildDemandDtl();
								bldDmdD.setVendorD(vendor);
								bldDmdD.setMatnrD(matnr);
								bldDmdD.setRecType1D(recType);
								bldDmdD.setPlantIDD(st.nextToken());
								bldDmdD.setThisRelCntT1D(thisReleaseType1RecsCount);
								bldDmdD.setThisRelCntT2D(thisReleaseType2RecsCount);
								bldDmdD.setThisRelCntT3D(thisReleaseType3RecsCount);
								bldDmdD.setThisRelNumD(thisReleaseNumber);
								fieldLoopCount++;
								bldDmdD.setPlantNameD(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setBeginDateD(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setEndDateD(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setSafetyStockD(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setCheckTotalD(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate1D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt1D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate2D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt2D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate3D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt3D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate4D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt4D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate5D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt5D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate6D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt6D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate7D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt7D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate8D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt8D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate9D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt9D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate10D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt10D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate11D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt11D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate12D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt12D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate13D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt13D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate14D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt14D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setbDate15D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setPbckt15D(st.nextToken());
								fieldLoopCount++;
								bldDmdD.setBaseUOMD(useThisUOM);
								bldDmdD.setOrderUOMD(useThisUOM);
								bldDmdD.setCustNumD("000000");
								bldDmdD.setDocRcvDateD(wbDate);
								bldDmdD.setDocRcvTimeD(String.valueOf(Util.getTimeStamp(cal)));
								bldDmdD.setSerializedRecNumD("0000000");
								bldDmdD.setPartnerIDD(YKKConstants.THIS_PARTNER);
								bldDmdD.setProcessFlag1D(" ");
								bldDmdD.setProcessFlag2D(" ");
								bldDmdD.setHolderField1D(" ");
								bldDmdD.setTimeStampD(tdStamp);
								dmdDtl1.add(bldDmdD);
							}
						}
						fieldLoopCount++;
						rCount++;
					}
				}
				yCount++;
				rCount++;
			}

			//transactionDefinition = new DefaultTransactionDefinition();
			//status = transactionManager.getTransaction(transactionDefinition);
			vfsRepository.saveVFSForecastData(dmdHdr1,dmdDtl1,dmdTxt1);
			//transactionManager.commit(status);
		} catch(IOException ioe) {
			ioe.printStackTrace();
			log.error("Error Caught  while reading file : " + ioe.getMessage());
			relNumber = null;
		}catch(SQLException se) {
			log.error("SQL Exception caught : " + se.getMessage());
			relNumber = null;
			//transactionManager.rollback(status);
		} finally {
			try {
				in.close();
			} catch(IOException ioe) {
				log.error("Error Caught while closing the input strean " );
				ioe.printStackTrace();
			}
		}

		return relNumber;
	}

	public void saveVFSStatus(String releaseNumber) {
		try {
			vfsRepository.saveVFSStatus(releaseNumber);
		} catch(SQLException e ) {
			log.error("Error while executing the SQL for YOUSTAT update");
		}
	}

}
