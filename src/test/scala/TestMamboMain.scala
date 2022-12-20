import com.ttne.mambo.core.MamboMain

class TestMamboMain extends TestBase {

  test("TestMamboMainArgs") {
      MamboMain.main(Array("src/test/resources/app.conf"))
  }
}
