package java.com.jh.fsduplicate.ui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.HeadlessException;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.com.jh.fsduplicate.FilesDAO;
import java.com.jh.fsduplicate.FsDuplicate;
import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;
import javax.swing.filechooser.FileFilter;
import javax.swing.text.BadLocationException;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyleContext;
import javax.swing.text.StyledDocument;

/**
 * @author <font size=-1 color="#a3a3a3">Johnny Hujol</font>
 * @since 6/11/12
 */
public final class Main extends JFrame implements ActionListener {

  private static final String STATUS_DIRECTORY_SUBMITTED = "STATUS_DIRECTORY_SUBMITTED";
  private static final String STATUS_DIRECTORY_VISITED = "STATUS_DIRECTORY_VISITED";

  private FilesDAO dao = null;
  private AtomicBoolean isSetup = new AtomicBoolean(false);
  private final JButton chooseDirectory;
  private final JTextPane jobsStatuses;

  public Main() throws HeadlessException {
    super("File System Analyzer");

    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

    final JPanel panel = new JPanel();
    panel.setLayout(new BorderLayout());

    chooseDirectory = new JButton("Choose directory...");
    final JPanel jPanelButtons = new JPanel();
    jPanelButtons.add(chooseDirectory);
    panel.add(jPanelButtons, BorderLayout.NORTH);
    jobsStatuses = new JTextPane();
    panel.add(new JScrollPane(jobsStatuses), BorderLayout.CENTER);

    setContentPane(panel);

    chooseDirectory.addActionListener(this);
  }

  public void setup() throws Exception {
    if(!isSetup.get()) {
      dao = new FilesDAO();
      isSetup.set(true);
    }
  }

  public void tearDown() {
    if(isSetup.get()) {
      dao.close();
      isSetup.set(false);
    }
  }

  private void showDirectoryChooser() {
    if(!EventQueue.isDispatchThread()) {
      throw new IllegalStateException(
          "Developer must call Main.showDirectoryChooser() method"
              + (EventQueue.isDispatchThread() ? " not" : "") + " from the EDT thread."
      );
    }

    final JFileChooser chooser = new JFileChooser();
    chooser.setFileFilter(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.isDirectory();
      }

      @Override
      public String getDescription() {
        return "Directory";
      }
    });
    chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
    chooser.setMultiSelectionEnabled(true);

    int i = chooser.showOpenDialog(this);
    if(JFileChooser.APPROVE_OPTION == i) {
      File[] selectedFiles = chooser.getSelectedFiles();
      for(final File selectedFile : selectedFiles) {
        updateJob(STATUS_DIRECTORY_SUBMITTED, selectedFile);
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              FsDuplicate.indexDirectory(dao, selectedFile);
            }
            catch(Exception e) {
              e.printStackTrace();
            }
            finally {
              Runnable runnable = new Runnable() {
                @Override
                public void run() {
                  updateJob(STATUS_DIRECTORY_VISITED, selectedFile);
                }
              };
              EventQueue.invokeLater(runnable);
            }
          }
        }).start();
      }
    }
  }

  private void updateJob(String status, File file) {
    if(!EventQueue.isDispatchThread()) {
      throw new IllegalStateException(
          "Developer must call Main.updateStatus() method"
              + (EventQueue.isDispatchThread() ? " not" : "") + " from the EDT thread."
      );
    }


    String line = status + ": " + file.getAbsolutePath();
    String style = STATUS_DIRECTORY_VISITED.equals(status) ? "job completed" : "running job";

    final StyledDocument doc = jobsStatuses.getStyledDocument();
    addStylesToDocument(doc);

    //        String[] initStyles = {
    //            "regular", "italic", "bold", "small", "large",
    //            "regular", "button", "regular", "icon",
    //            "regular"};

    // Load the text pane with styled text.
    try {
      doc.insertString(doc.getLength(), line, doc.getStyle(style));
      doc.insertString(doc.getLength(), "\n", doc.getStyle("running job"));
    }
    catch(BadLocationException ble) {
      System.err.println("Couldn't insert initial text into text pane.");
    }
  }

  private static void addStylesToDocument(StyledDocument doc) {
    // Initialize some styles.
    Style regular = doc.getStyle("running job");

    if(null == regular) {
      Style def = StyleContext.getDefaultStyleContext().getStyle(StyleContext.DEFAULT_STYLE);
      regular = doc.addStyle("running job", def);
      StyleConstants.setFontFamily(def, "SansSerif");
      StyleConstants.setForeground(def, Color.green);
    }

    // Check if a job completed style exists.
    Style s = doc.getStyle("job completed");
    if(null == s) {
      s = doc.addStyle("job completed", regular);
      StyleConstants.setBold(s, true);
      StyleConstants.setForeground(s, Color.red);
    }

    /*
            s = doc.addStyle("italic", regular);
            StyleConstants.setItalic(s, true);

            s = doc.addStyle("small", regular);
            StyleConstants.setFontSize(s, 10);

            s = doc.addStyle("large", regular);
            StyleConstants.setFontSize(s, 16);
    */

    //        s = doc.addStyle("icon", regular);
    //        StyleConstants.setAlignment(s, StyleConstants.ALIGN_CENTER);
    //        ImageIcon pigIcon = createImageIcon("images/Pig.gif",
    //            "a cute pig");
    //        if(pigIcon != null) {
    //            StyleConstants.setIcon(s, pigIcon);
    //        }

    //        s = doc.addStyle("button", regular);
    //        StyleConstants.setAlignment(s, StyleConstants.ALIGN_CENTER);
    //        ImageIcon soundIcon = createImageIcon("images/sound.gif",
    //            "sound icon");
    //        JButton button = new JButton();
    //        if(soundIcon != null) {
    //            button.setIcon(soundIcon);
    //        }
    //        else {
    //            button.setText("BEEP");
    //        }
    //        button.setCursor(Cursor.getDefaultCursor());
    //        button.setMargin(new Insets(0, 0, 0, 0));
    //        button.setActionCommand(buttonString);
    //        button.addActionListener(this);
    //        StyleConstants.setComponent(s, button);
  }

  public static void main(String[] args) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          Main ds = new Main();
          ds.setup();

          ds.setSize(new Dimension(600, 400));
          ds.setLocationRelativeTo(null);
          ds.setVisible(true);
        }
        catch(Exception e) {
          e.printStackTrace();
        }
      }
    };
    EventQueue.invokeLater(runnable);
  }

  @Override
  public void actionPerformed(ActionEvent actionEvent) {
    if(chooseDirectory == actionEvent.getSource()) { // == on purpose
      showDirectoryChooser();
    }
  }
}
